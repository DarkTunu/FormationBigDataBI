"""Build conformed dimensions and six eBrasil marts from the bronze Parquet set.

Writes analytics tables as Parquet below lakehouse/silver/ebrasil and JSON
copies for Druid batch ingestion. The source Parquet files are authoritative.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType


ROOT = "s3a://lakehouse"
BRONZE = f"{ROOT}/bronze/ebrasil"
SILVER = f"{ROOT}/silver/ebrasil"
INPUTS = ("customers", "geolocation", "items", "orders", "payments",
          "products", "reviews", "sellers")
DRUID_METRICS = {
    "fact_order_item": {"price": "doubleSum", "freight_value": "doubleSum",
                        "line_total": "doubleSum", "gmv": "doubleSum", "item_count": "longSum"},
    "fact_order": {"item_count": "longSum", "product_amount": "doubleSum",
                   "freight_amount": "doubleSum", "payment_amount": "doubleSum",
                   "payment_gap": "doubleSum"},
    "fact_payment": {"payment_value": "doubleSum", "installments": "longSum",
                     "transaction_count": "longSum"},
    "fact_logistics": {"distance_km": "doubleSum", "delivery_days": "doubleSum",
                       "late_days": "doubleSum", "delivered_on_time": "longSum",
                       "orders_count": "longSum"},
    "fact_review": {"review_count": "longSum", "score_sum": "longSum",
                    "score_1": "longSum", "score_2": "longSum", "score_3": "longSum",
                    "score_4": "longSum", "score_5": "longSum",
                    "text_review_groups": "longSum", "answer_hours_sum": "doubleSum",
                    "creation_hours_sum": "doubleSum"},
    "mart_customer_rfm": {"frequency": "longSum", "monetary": "doubleSum",
                          "recency_days": "longSum", "average_order_value": "doubleSum"},
}

def read(name):
    path = f"{BRONZE}/{name}.parquet"
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Cannot read required bronze input {path}: {exc}") from exc

def has(df, name):
    return name in df.columns

def col_or_null(df, name, alias=None, dtype="string"):
    value = F.col(name).cast(dtype) if has(df, name) else F.lit(None).cast(dtype)
    return value.alias(alias or name)

def timestamp(df, name):
    if not has(df, name):
        return F.lit(None).cast("timestamp")
    if isinstance(df.schema[name].dataType, LongType):
        # Spark's compatibility mode exposes Parquet nanoseconds as epoch-ns longs.
        # Convert to epoch microseconds before building a Spark timestamp.
        return F.timestamp_micros(F.expr(f"`{name}` DIV 1000"))
    return F.to_timestamp(F.col(name))

def key_date(ts):
    return F.date_format(ts, "yyyyMMdd").cast("int")

def key_time(ts):
    return (F.hour(ts) * 10000 + F.minute(ts) * 100 + F.second(ts)).cast("int")

def metric_fields(name):
    return DRUID_METRICS.get(name, {})

def persist(name, df, manifest):
    df = df.withColumn("event_ts", F.coalesce(F.col("event_ts").cast("string"),
                          F.lit("1970-01-01T00:00:00Z")))
    df.write.mode("overwrite").parquet(f"{SILVER}/{name}")
    count = df.count()
    druid_path = f"{SILVER}/druid/{name}"
    # Druid's S3 input enumerates every JSON object under this table prefix.
    # Keep Spark's partitions so large facts do not concentrate in one task.
    df.write.mode("overwrite").json(druid_path)
    metrics = metric_fields(name)
    manifest[name] = {
        "rows": count,
        "parquet_path": f"silver/ebrasil/{name}",
        "druid_path": f"silver/ebrasil/druid/{name}",
        "dimensions": [c for c in df.columns if c != "event_ts" and c not in metrics],
        "metrics": metrics,
    }
    print(f"EBRASIL_TABLE_OK name={name} rows={count}", flush=True)

def main():
    global spark
    spark = (SparkSession.builder.appName("ebrasil-star-schema")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.parquet.nanosAsLong", "true").getOrCreate())
    hadoop_conf = spark._jsc.hadoopConfiguration()
    missing = []
    for name in INPUTS:
        path = spark._jvm.org.apache.hadoop.fs.Path(f"{BRONZE}/{name}.parquet")
        # Resolve the filesystem from the s3a:// URI instead of Hadoop's default
        # otherwise Hadoop may select the local filesystem and reject S3A paths.
        if not path.getFileSystem(hadoop_conf).exists(path):
            missing.append(name)
    if missing:
        raise RuntimeError(f"Missing bronze parquet inputs: {missing}")

    customers, geo, items, orders, payments, products, reviews, sellers = (
        read(n) for n in INPUTS
    )
    manifest = {}

    # Conformed geography uses the provided zip-code centroids and ranges.
    geo_select = [col_or_null(geo, "zip_code", dtype="string"),
                  col_or_null(geo, "city"), col_or_null(geo, "state")]
    for field in ("lat", "lng", "lat_min", "lat_max", "lng_min", "lng_max"):
        geo_select.append(col_or_null(geo, field, dtype="double"))
    dim_geo = geo.select(*geo_select).dropDuplicates(["zip_code"]).withColumn(
        "event_ts", F.lit("1970-01-01T00:00:00Z"))
    persist("dim_geography", dim_geo, manifest)

    customer_base = customers.select(
        col_or_null(customers, "customer_unique_id"),
        col_or_null(customers, "customer_id"),
        col_or_null(customers, "zip_code", dtype="string"),
        col_or_null(customers, "city"), col_or_null(customers, "state"),
        col_or_null(customers, "name_state"))
    dim_customer = (customer_base.alias("c").join(
        geo.select(col_or_null(geo, "zip_code", dtype="string"),
                   col_or_null(geo, "lat", dtype="double"),
                   col_or_null(geo, "lng", dtype="double")).alias("g"),
        F.col("c.zip_code") == F.col("g.zip_code"), "left")
        .select("c.customer_unique_id", "c.customer_id", "c.zip_code", "c.city", "c.state",
                "c.name_state", F.col("g.lat").alias("latitude"),
                F.col("g.lng").alias("longitude"))
        .dropDuplicates(["customer_unique_id"])
        .withColumn("event_ts", F.lit("1970-01-01T00:00:00Z")))
    persist("dim_customer", dim_customer, manifest)

    dim_product = products.select(
        col_or_null(products, "product_id"),
        col_or_null(products, "category_name"),
        *[col_or_null(products, f, dtype="double") for f in
          ("name_length", "description_length", "photos_qty", "weight_g",
           "length_cm", "height_cm", "width_cm")]
    ).dropDuplicates(["product_id"]).withColumn("event_ts", F.lit("1970-01-01T00:00:00Z"))
    persist("dim_product", dim_product, manifest)

    dim_seller = sellers.select(
        col_or_null(sellers, "seller_id"), col_or_null(sellers, "zip_code", dtype="string"),
        col_or_null(sellers, "city"), col_or_null(sellers, "state"),
        col_or_null(sellers, "name_state")
    ).dropDuplicates(["seller_id"]).withColumn("event_ts", F.lit("1970-01-01T00:00:00Z"))
    persist("dim_seller", dim_seller, manifest)

    # Order source already exposes named milestones from the existing notebook.
    order_columns = ["order_id", "customer_id", "status", "purchase_timestamp",
                     "approved_at", "delivered_carrier", "delivered_customer",
                     "estimated_delivery"]
    order_timestamp_columns = {
        "purchase_timestamp", "approved_at", "delivered_carrier",
        "delivered_customer", "estimated_delivery",
    }
    o = orders.select(*[
        timestamp(orders, n).alias(n) if n in order_timestamp_columns
        else col_or_null(orders, n)
        for n in order_columns
    ])
    purchase = timestamp(o, "purchase_timestamp")
    date_frames = [o.select(timestamp(o, n).alias("d")) for n in
                   ("purchase_timestamp", "approved_at", "delivered_carrier",
                    "delivered_customer", "estimated_delivery")]
    date_values = reduce(lambda a, b: a.unionByName(b), date_frames).where(F.col("d").isNotNull())
    dim_date = (date_values.select(F.to_date("d").alias("calendar_date")).distinct()
        .select(F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
                F.col("calendar_date").cast("string").alias("calendar_date"),
                F.year("calendar_date").alias("year"),
                F.when(F.month("calendar_date") <= 6, 1).otherwise(2).alias("semester"),
                F.quarter("calendar_date").alias("quarter"),
                F.month("calendar_date").alias("month"),
                F.date_format("calendar_date", "MMMM").alias("month_name"),
                F.weekofyear("calendar_date").alias("iso_week"),
                F.dayofmonth("calendar_date").alias("day"),
                F.dayofweek("calendar_date").alias("weekday"),
                F.when(F.dayofweek("calendar_date").isin(1, 7), True).otherwise(False).alias("is_weekend"))
        .withColumn("event_ts", F.concat(F.col("calendar_date"), F.lit("T00:00:00Z"))))
    persist("dim_date", dim_date, manifest)

    times = reduce(lambda a, b: a.unionByName(b),
        [o.select(timestamp(o, n).alias("t")) for n in
         ("purchase_timestamp", "approved_at", "delivered_carrier",
          "delivered_customer", "estimated_delivery")]).where(F.col("t").isNotNull())
    dim_time = (times.select(key_time(F.col("t")).alias("time_key")).distinct()
        .select("time_key", (F.col("time_key") / 10000).cast("int").alias("hour"),
                F.floor((F.col("time_key") % 10000) / 100).cast("int").alias("minute"),
                (F.col("time_key") % 100).cast("int").alias("second"),
                F.when(F.col("time_key") < 60000, "night")
                 .when(F.col("time_key") < 120000, "morning")
                 .when(F.col("time_key") < 180000, "afternoon").otherwise("evening")
                 .alias("day_part"))
        .withColumn("event_ts", F.concat(F.lit("2000-01-01T"),
             F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":"),
             F.lpad(F.col("minute").cast("string"), 2, "0"), F.lit(":"),
             F.lpad(F.col("second").cast("string"), 2, "0"), F.lit("Z"))))
    persist("dim_time", dim_time, manifest)

    dim_status = (o.select(F.coalesce(F.col("status"), F.lit("unknown")).alias("status"))
                  .distinct().withColumn("event_ts", F.lit("1970-01-01T00:00:00Z")))
    persist("dim_order_status", dim_status, manifest)

    # Build facts from the prepared source tables, preserving their grains.
    cust = customers.select("customer_id", "customer_unique_id",
        F.col("zip_code").cast("string").alias("customer_zip"),
        F.col("city").alias("customer_city"), F.col("state").alias("customer_state"))
    item_cols = [F.col("order_id"), F.col("product_id"), F.col("seller_id"),
                 F.col("price").cast("double").alias("price"),
                 F.col("freight_value").cast("double").alias("freight_value")]
    if has(items, "order_item_id"):
        item_cols.append(F.col("order_item_id"))
    i = items.select(*item_cols)
    item_enriched = (i.join(o, "order_id", "left").join(cust, "customer_id", "left")
        .join(products.select("product_id", "category_name"), "product_id", "left")
        .join(sellers.select("seller_id", F.col("zip_code").cast("string").alias("seller_zip"),
                             F.col("city").alias("seller_city"),
                             F.col("state").alias("seller_state")), "seller_id", "left"))
    sales = item_enriched.select(
        F.col("order_id"), F.col("order_item_id") if "order_item_id" in item_enriched.columns else F.lit(1).alias("order_item_id"),
        F.col("purchase_timestamp"), key_date(purchase if "purchase_timestamp" in o.columns else F.lit(None)).alias("purchase_date_key"),
        key_time(purchase).alias("purchase_time_key"), F.col("customer_unique_id"),
        F.col("product_id"), F.col("category_name"), F.col("seller_id"),
        F.col("price"), F.col("freight_value"), (F.col("price") + F.col("freight_value")).alias("line_total"),
        F.col("price").alias("gmv"), F.lit(1).cast("long").alias("item_count"),
        F.concat(F.date_format(purchase, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z")).alias("event_ts"))
    persist("fact_order_item", sales, manifest)

    item_order = i.groupBy("order_id").agg(
        F.count("*").cast("long").alias("item_count"),
        F.sum("price").alias("product_amount"), F.sum("freight_value").alias("freight_amount"),
        F.countDistinct("product_id").cast("long").alias("product_count"),
        F.countDistinct("seller_id").cast("long").alias("seller_count"))
    pay_value = F.col("value").cast("double") if has(payments, "value") else reduce(
        lambda a, b: a + b, [F.coalesce(F.col(c).cast("double"), F.lit(0.0))
        for c in payments.columns if c.startswith("value_")])
    pay_order = payments.select("order_id", pay_value.alias("payment_amount")).groupBy(
        "order_id").agg(F.sum("payment_amount").alias("payment_amount"))
    order_fact = (o.join(item_order, "order_id", "left").join(pay_order, "order_id", "left")
        .join(cust, "customer_id", "left").withColumn("product_amount", F.coalesce("product_amount", F.lit(0.0)))
        .withColumn("freight_amount", F.coalesce("freight_amount", F.lit(0.0)))
        .withColumn("payment_amount", F.coalesce("payment_amount", F.lit(0.0)))
        .withColumn("payment_gap", F.col("payment_amount")-F.col("product_amount")-F.col("freight_amount"))
        .select("order_id", "customer_unique_id", "status",
                key_date(purchase).alias("purchase_date_key"),
                key_date(F.col("approved_at")).alias("approved_date_key"),
                key_date(F.col("delivered_carrier")).alias("shipped_date_key"),
                key_date(F.col("delivered_customer")).alias("delivered_date_key"),
                key_date(F.col("estimated_delivery")).alias("estimated_delivery_date_key"),
                key_time(purchase).alias("purchase_time_key"),
                key_time(F.col("approved_at")).alias("approved_time_key"),
                key_time(F.col("delivered_carrier")).alias("shipped_time_key"),
                key_time(F.col("delivered_customer")).alias("delivered_time_key"),
                "item_count", "product_count", "seller_count", "product_amount", "freight_amount",
                "payment_amount", "payment_gap",
                F.concat(F.date_format(purchase, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z")).alias("event_ts")))
    persist("fact_order", order_fact, manifest)

    # The prepared payments.parquet is pivoted by order in this repository, so
    # the finest possible grain is order x payment method, not payment sequence.
    if has(payments, "type") and has(payments, "payment_sequential"):
        pay_detail = payments.select("order_id",
            F.col("type").alias("payment_type"), F.col("value").cast("double").alias("payment_value"),
            F.col("payment_installments").cast("long").alias("installments"))
    else:
        value_columns = [c for c in payments.columns if c.startswith("value_")]
        if value_columns:
            payment_rows = []
            for value_column in value_columns:
                payment_type = value_column[6:]
                installment_column = f"int_{payment_type}"
                installment_value = (F.col(installment_column).cast("long")
                                     if has(payments, installment_column)
                                     else F.lit(0).cast("long"))
                payment_rows.append(payments.select("order_id",
                    F.lit(payment_type).alias("payment_type"),
                    F.col(value_column).cast("double").alias("payment_value"),
                    installment_value.alias("installments")))
            pay_detail = reduce(lambda a, b: a.unionByName(b), payment_rows)
        else:
            pay_detail = payments.select("order_id", F.lit("aggregated").alias("payment_type"),
                F.col("value").cast("double").alias("payment_value"),
                F.lit(0).cast("long").alias("installments"))
    pay_detail = pay_detail.where(F.coalesce("payment_value", F.lit(0.0)) != 0)
    pay_dim = pay_detail.select("payment_type").distinct().withColumn(
        "event_ts", F.lit("1970-01-01T00:00:00Z"))
    persist("dim_payment_type", pay_dim, manifest)
    finance = (pay_detail.join(o, "order_id", "left").join(cust, "customer_id", "left")
        .select("order_id", "payment_type", "customer_unique_id",
                key_date(purchase).alias("purchase_date_key"), key_time(purchase).alias("purchase_time_key"),
                "payment_value", "installments", F.lit(1).cast("long").alias("transaction_count"),
                F.concat(F.date_format(purchase, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z")).alias("event_ts")))
    persist("fact_payment", finance, manifest)

    # Logistics grain is one order. When an order has multiple sellers, keep
    # seller_count and use the first available seller coordinate for distance.
    cust_geo = customer_base.select("customer_id", F.col("zip_code").cast("string").alias("customer_zip"))
    cust_coords = geo.select(F.col("zip_code").cast("string").alias("czip"),
        F.col("lat").cast("double").alias("clat"), F.col("lng").cast("double").alias("clng"))
    seller_first = i.join(sellers.select("seller_id", F.col("zip_code").cast("string").alias("seller_zip")),
        "seller_id", "left").groupBy("order_id").agg(F.first("seller_zip", ignorenulls=True).alias("seller_zip"),
                                                     F.countDistinct("seller_id").alias("seller_count"))
    seller_coords = geo.select(F.col("zip_code").cast("string").alias("szip"),
        F.col("lat").cast("double").alias("slat"), F.col("lng").cast("double").alias("slng"))
    logistics = (o.join(cust_geo, "customer_id", "left").join(cust_coords,
        F.col("customer_zip")==F.col("czip"), "left").join(seller_first, "order_id", "left")
        .join(seller_coords, F.col("seller_zip")==F.col("szip"), "left"))
    radians = F.lit(0.017453292519943295)
    haversine = 6371.0088 * 2 * F.asin(F.sqrt(
        F.pow(F.sin((F.col("slat")-F.col("clat"))*radians/2),2) +
        F.cos(F.col("clat")*radians)*F.cos(F.col("slat")*radians)*
        F.pow(F.sin((F.col("slng")-F.col("clng"))*radians/2),2)))
    logistics = logistics.select("order_id", "customer_id", "customer_zip", "seller_zip",
        "status", key_date(purchase).alias("purchase_date_key"),
        key_date(F.col("approved_at")).alias("approved_date_key"),
        key_date(F.col("delivered_carrier")).alias("shipped_date_key"),
        key_date(F.col("delivered_customer")).alias("delivered_date_key"),
        key_date(F.col("estimated_delivery")).alias("estimated_delivery_date_key"),
        key_time(purchase).alias("purchase_time_key"),
        key_time(F.col("delivered_customer")).alias("delivered_time_key"),
        haversine.alias("distance_km"),
        ((F.unix_timestamp("delivered_customer")-F.unix_timestamp("purchase_timestamp"))/86400.0)
            .alias("delivery_days"),
        ((F.unix_timestamp("delivered_customer")-F.unix_timestamp("estimated_delivery"))/86400.0)
            .alias("late_days"),
        F.when(F.col("delivered_customer").isNotNull(),
            F.when(F.col("delivered_customer") <= F.col("estimated_delivery"), 1).otherwise(0))
            .otherwise(0).cast("long").alias("delivered_on_time"),
        F.lit(1).cast("long").alias("orders_count"),
        F.concat(F.date_format(purchase, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z")).alias("event_ts"))
    persist("fact_logistics", logistics, manifest)

    # Reviews are order-level in the prepared Parquet. Preserve score counts
    # and compute an average only from the observed distribution.
    score_columns = [f"score_{n}" for n in range(1, 6) if f"score_{n}" in reviews.columns]
    if score_columns:
        score_counts = {n: F.coalesce(F.col(f"score_{n}").cast("long"), F.lit(0))
                        for n in range(1, 6) if f"score_{n}" in reviews.columns}
        review_count = reduce(lambda a, b: a+b, list(score_counts.values()))
        score_sum = reduce(lambda a, b: a+b, [
            score_counts[n] * F.lit(n) for n in score_counts])
        comment_columns = [f"comment_{n}" for n in range(1, 6)
                           if f"comment_{n}" in reviews.columns]
        answer_columns = [f"answer_{n}" for n in range(1, 6)
                          if f"answer_{n}" in reviews.columns]
        creation_columns = [f"creation_{n}" for n in range(1, 6)
                            if f"creation_{n}" in reviews.columns]
        text_groups = reduce(lambda a, b: a+b, [
            F.when(F.coalesce(F.col(name).cast("double"), F.lit(0.0)) > 0, 1)
             .otherwise(0).cast("long") for name in comment_columns
        ]) if comment_columns else F.lit(0).cast("long")
        answer_sum = reduce(lambda a, b: a+b, [
            F.coalesce(F.col(name).cast("double"), F.lit(0.0)) for name in answer_columns
        ]) if answer_columns else F.lit(0.0)
        creation_sum = reduce(lambda a, b: a+b, [
            F.coalesce(F.col(name).cast("double"), F.lit(0.0)) for name in creation_columns
        ]) if creation_columns else F.lit(0.0)
        review_fact = reviews.select("order_id", review_count.alias("review_count"),
            score_sum.alias("score_sum"), text_groups.alias("text_review_groups"),
            answer_sum.alias("answer_hours_sum"), creation_sum.alias("creation_hours_sum"),
            *[score_counts[n].alias(f"score_{n}") for n in score_counts])
    elif has(reviews, "score"):
        review_fact = reviews.select("order_id", F.lit(1).cast("long").alias("review_count"),
            F.col("score").cast("long").alias("score_sum"),
            F.when((F.col("comment").isNotNull()) & (F.col("comment") > 0), 1).otherwise(0)
                .cast("long").alias("text_review_groups"),
            F.col("answer").cast("double").alias("answer_hours_sum"),
            F.col("creation").cast("double").alias("creation_hours_sum"))
    else:
        raise RuntimeError(f"Unsupported reviews.parquet schema: {reviews.columns}")
    review_fact = (review_fact.join(o, "order_id", "left").join(cust, "customer_id", "left")
        .select("order_id", "customer_unique_id", "status", "review_count", "score_sum",
            F.when(F.col("review_count")>0, F.col("score_sum")/F.col("review_count"))
             .alias("average_score"), "text_review_groups", "answer_hours_sum", "creation_hours_sum",
            *[F.col(f"score_{n}") for n in range(1, 6) if f"score_{n}" in review_fact.columns],
            key_date(purchase).alias("purchase_date_key"), key_time(purchase).alias("purchase_time_key"),
            F.concat(F.date_format(purchase, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z")).alias("event_ts")))
    persist("fact_review", review_fact, manifest)

    delivered = o.where(F.lower(F.col("status")) == "delivered")
    rfm = (delivered.join(cust, "customer_id", "left").join(item_order, "order_id", "left")
        .groupBy("customer_unique_id").agg(
            F.max("purchase_timestamp").alias("last_purchase"),
            F.countDistinct("order_id").cast("long").alias("frequency"),
            F.sum(F.coalesce("product_amount", F.lit(0.0))+F.coalesce("freight_amount", F.lit(0.0)))
                .alias("monetary"),
            F.avg(F.coalesce("product_amount", F.lit(0.0))+F.coalesce("freight_amount", F.lit(0.0)))
                .alias("average_order_value"))
        .withColumn("recency_days", F.datediff(F.lit(datetime.now(timezone.utc).date().isoformat()),
                                               F.to_date("last_purchase")).cast("long"))
        .withColumn("event_ts", F.concat(F.date_format("last_purchase", "yyyy-MM-dd'T'HH:mm:ss"), F.lit("Z"))))
    persist("mart_customer_rfm", rfm, manifest)

    manifest_doc = {
        "model": "eBrasil Olist dimensional constellation",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_prefix": "bronze/ebrasil",
        "silver_prefix": "silver/ebrasil",
        "source_files": [f"{n}.parquet" for n in INPUTS],
        "source_grain_limits": {
            "payments": "Prepared payments.parquet is pivoted by order; finance grain is order x payment method.",
            "reviews": "Prepared reviews.parquet is aggregated by order; review score distribution is retained."
        },
        "tables": manifest
    }
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{SILVER}/model-manifest.json")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(path, True)
    stream.write(bytearray(json.dumps(manifest_doc, ensure_ascii=False, sort_keys=True).encode("utf-8")))
    stream.close()
    print("EBRASIL_SILVER_MODEL_OK " + json.dumps({k:v["rows"] for k,v in manifest.items()}, sort_keys=True),
          flush=True)
    spark.stop()

if __name__ == "__main__":
    main()
