"""Build the relational eBrasilDW star warehouse from the repository CSV sources.

Every source timestamp is truncated to the hour before deriving date/time keys.
The complete schema refresh runs in one database transaction and is repeatable.
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql


DATA_DIR = Path(os.getenv("EBRASIL_CSV_DIR", "/data/ebrasil"))
DATABASE = os.getenv("EBRASIL_DW_DATABASE", "ebrasilDW")
ADMIN_DB = os.getenv("PGDATABASE", "formation")
PG = {
    "host": os.getenv("PGHOST", "postgres-source"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "formation"),
    "password": os.environ["PGPASSWORD"],
}

SOURCES = {
    "customers": (
        "olist_customers_dataset.csv",
        ("customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"),
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
         "geolocation_city", "geolocation_state"),
    ),
    "items": (
        "olist_order_items_dataset.csv",
        ("order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"),
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        ("order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"),
    ),
    "reviews": (
        "olist_order_reviews_dataset.csv",
        ("review_id", "order_id", "review_score", "review_comment_title",
         "review_comment_message", "review_creation_date",
         "review_answer_timestamp"),
    ),
    "orders": (
        "olist_orders_dataset.csv",
        ("order_id", "customer_id", "order_status",
         "order_purchase_timestamp", "order_approved_at",
         "order_delivered_carrier_date", "order_delivered_customer_date",
         "order_estimated_delivery_date"),
    ),
    "products": (
        "olist_products_dataset.csv",
        ("product_id", "product_category_name", "product_name_lenght",
         "product_description_lenght", "product_photos_qty",
         "product_weight_g", "product_length_cm", "product_height_cm",
         "product_width_cm"),
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
    ),
    "translation": (
        "product_category_name_translation.csv",
        ("product_category_name", "product_category_name_english"),
    ),
}

DDL = (
    """CREATE TABLE dw.dim_geography (
        zip_code_prefix text PRIMARY KEY,
        city text NOT NULL,
        state text NOT NULL,
        latitude double precision,
        longitude double precision
    )""",
    """CREATE TABLE dw.dim_customer (
        customer_unique_id text PRIMARY KEY,
        representative_customer_id text NOT NULL,
        zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        city text NOT NULL,
        state text NOT NULL
    )""",
    """CREATE TABLE dw.dim_product (
        product_id text PRIMARY KEY,
        category_name text NOT NULL,
        category_name_english text,
        name_length integer,
        description_length integer,
        photos_qty integer,
        weight_g numeric(12, 3),
        length_cm numeric(12, 3),
        height_cm numeric(12, 3),
        width_cm numeric(12, 3)
    )""",
    """CREATE TABLE dw.dim_seller (
        seller_id text PRIMARY KEY,
        zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        city text NOT NULL,
        state text NOT NULL
    )""",
    """CREATE TABLE dw.dim_order_status (
        order_status text PRIMARY KEY
    )""",
    """CREATE TABLE dw.dim_payment_type (
        payment_type text PRIMARY KEY
    )""",
    """CREATE TABLE dw.dim_review_score (
        review_score smallint PRIMARY KEY,
        score_label text NOT NULL
    )""",
    """CREATE TABLE dw.dim_date (
        date_key integer PRIMARY KEY,
        calendar_date date NOT NULL UNIQUE,
        year smallint NOT NULL,
        semester smallint NOT NULL,
        quarter smallint NOT NULL,
        month smallint NOT NULL,
        month_name text NOT NULL,
        iso_week smallint NOT NULL,
        day smallint NOT NULL,
        weekday smallint NOT NULL,
        is_weekend boolean NOT NULL
    )""",
    """CREATE TABLE dw.dim_time (
        time_key integer PRIMARY KEY,
        hour smallint NOT NULL UNIQUE CHECK (hour BETWEEN 0 AND 23),
        day_part text NOT NULL
    )""",
    """CREATE TABLE dw.fact_order_item (
        order_id text NOT NULL,
        order_item_id integer NOT NULL,
        customer_unique_id text NOT NULL REFERENCES dw.dim_customer(customer_unique_id),
        product_id text NOT NULL REFERENCES dw.dim_product(product_id),
        seller_id text NOT NULL REFERENCES dw.dim_seller(seller_id),
        order_status text NOT NULL REFERENCES dw.dim_order_status(order_status),
        customer_zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        seller_zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        purchase_date_key integer NOT NULL REFERENCES dw.dim_date(date_key),
        purchase_time_key integer NOT NULL REFERENCES dw.dim_time(time_key),
        shipping_limit_date_key integer REFERENCES dw.dim_date(date_key),
        shipping_limit_time_key integer REFERENCES dw.dim_time(time_key),
        price numeric(14, 2) NOT NULL CHECK (price >= 0),
        freight_value numeric(14, 2) NOT NULL CHECK (freight_value >= 0),
        line_total numeric(14, 2) NOT NULL,
        gmv numeric(14, 2) NOT NULL,
        item_count integer NOT NULL CHECK (item_count = 1),
        event_ts timestamp NOT NULL,
        PRIMARY KEY (order_id, order_item_id)
    )""",
    """CREATE TABLE dw.fact_order (
        order_id text PRIMARY KEY,
        customer_unique_id text NOT NULL REFERENCES dw.dim_customer(customer_unique_id),
        order_status text NOT NULL REFERENCES dw.dim_order_status(order_status),
        purchase_date_key integer NOT NULL REFERENCES dw.dim_date(date_key),
        purchase_time_key integer NOT NULL REFERENCES dw.dim_time(time_key),
        approved_date_key integer REFERENCES dw.dim_date(date_key),
        approved_time_key integer REFERENCES dw.dim_time(time_key),
        shipped_date_key integer REFERENCES dw.dim_date(date_key),
        shipped_time_key integer REFERENCES dw.dim_time(time_key),
        delivered_date_key integer REFERENCES dw.dim_date(date_key),
        delivered_time_key integer REFERENCES dw.dim_time(time_key),
        estimated_delivery_date_key integer REFERENCES dw.dim_date(date_key),
        estimated_delivery_time_key integer REFERENCES dw.dim_time(time_key),
        item_count bigint NOT NULL,
        product_count bigint NOT NULL,
        seller_count bigint NOT NULL,
        product_amount numeric(16, 2) NOT NULL,
        freight_amount numeric(16, 2) NOT NULL,
        payment_amount numeric(16, 2) NOT NULL,
        payment_gap numeric(16, 2) NOT NULL,
        event_ts timestamp NOT NULL
    )""",
    """CREATE TABLE dw.fact_payment (
        order_id text NOT NULL,
        payment_sequential integer NOT NULL,
        customer_unique_id text NOT NULL REFERENCES dw.dim_customer(customer_unique_id),
        payment_type text NOT NULL REFERENCES dw.dim_payment_type(payment_type),
        purchase_date_key integer NOT NULL REFERENCES dw.dim_date(date_key),
        purchase_time_key integer NOT NULL REFERENCES dw.dim_time(time_key),
        payment_value numeric(14, 2) NOT NULL,
        installments integer NOT NULL,
        transaction_count integer NOT NULL CHECK (transaction_count = 1),
        event_ts timestamp NOT NULL,
        PRIMARY KEY (order_id, payment_sequential)
    )""",
    """CREATE TABLE dw.fact_logistics (
        order_id text PRIMARY KEY,
        customer_unique_id text NOT NULL REFERENCES dw.dim_customer(customer_unique_id),
        representative_seller_id text REFERENCES dw.dim_seller(seller_id),
        customer_zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        seller_zip_code_prefix text NOT NULL REFERENCES dw.dim_geography(zip_code_prefix),
        order_status text NOT NULL REFERENCES dw.dim_order_status(order_status),
        purchase_date_key integer NOT NULL REFERENCES dw.dim_date(date_key),
        purchase_time_key integer NOT NULL REFERENCES dw.dim_time(time_key),
        approved_date_key integer REFERENCES dw.dim_date(date_key),
        approved_time_key integer REFERENCES dw.dim_time(time_key),
        shipped_date_key integer REFERENCES dw.dim_date(date_key),
        shipped_time_key integer REFERENCES dw.dim_time(time_key),
        delivered_date_key integer REFERENCES dw.dim_date(date_key),
        delivered_time_key integer REFERENCES dw.dim_time(time_key),
        estimated_delivery_date_key integer REFERENCES dw.dim_date(date_key),
        estimated_delivery_time_key integer REFERENCES dw.dim_time(time_key),
        distance_km numeric(14, 3),
        delivery_days numeric(12, 4),
        late_days numeric(12, 4),
        delivered_on_time smallint NOT NULL CHECK (delivered_on_time IN (0, 1)),
        orders_count integer NOT NULL CHECK (orders_count = 1),
        event_ts timestamp NOT NULL
    )""",
    """CREATE TABLE dw.fact_review (
        review_id text PRIMARY KEY,
        order_id text NOT NULL,
        customer_unique_id text NOT NULL REFERENCES dw.dim_customer(customer_unique_id),
        order_status text NOT NULL REFERENCES dw.dim_order_status(order_status),
        review_score smallint NOT NULL REFERENCES dw.dim_review_score(review_score),
        creation_date_key integer REFERENCES dw.dim_date(date_key),
        creation_time_key integer REFERENCES dw.dim_time(time_key),
        answer_date_key integer REFERENCES dw.dim_date(date_key),
        answer_time_key integer REFERENCES dw.dim_time(time_key),
        review_count integer NOT NULL CHECK (review_count = 1),
        score_sum integer NOT NULL,
        has_comment boolean NOT NULL,
        response_hours numeric(12, 3),
        event_ts timestamp NOT NULL
    )""",
    """CREATE TABLE dw.mart_customer_rfm (
        customer_unique_id text PRIMARY KEY REFERENCES dw.dim_customer(customer_unique_id),
        last_purchase_date_key integer NOT NULL REFERENCES dw.dim_date(date_key),
        last_purchase_time_key integer NOT NULL REFERENCES dw.dim_time(time_key),
        frequency bigint NOT NULL,
        monetary numeric(16, 2) NOT NULL,
        recency_days numeric(12, 4) NOT NULL,
        average_order_value numeric(16, 2) NOT NULL
    )""",
)


def ts(column: str) -> str:
    return f"date_trunc('hour', NULLIF({column}, '')::timestamp)"


def date_key(expr: str) -> str:
    return f"to_char(({expr})::date, 'YYYYMMDD')::integer"


def time_key(expr: str) -> str:
    return f"(extract(hour FROM ({expr}))::integer * 10000)"


def ensure_database() -> None:
    admin = psycopg.connect(dbname=ADMIN_DB, autocommit=True, **PG)
    try:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,)
        ).fetchone()
        if not exists:
            admin.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE))
            )
    finally:
        admin.close()


def stage_sources(conn: psycopg.Connection) -> None:
    for name, (filename, columns) in SOURCES.items():
        path = DATA_DIR / filename
        if not path.is_file():
            raise FileNotFoundError(f"Required eBrasil source file is missing: {path}")
        table = f"stg_{name}"
        column_ddl = ", ".join(f"{column} text" for column in columns)
        conn.execute(
            sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
                sql.Identifier(table), sql.SQL(column_ddl)
            )
        )
        copy = sql.SQL(
            "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')"
        ).format(sql.Identifier(table))
        with path.open("r", encoding="utf-8", newline="") as source:
            with conn.cursor().copy(copy) as writer:
                while chunk := source.read(1024 * 1024):
                    writer.write(chunk)
        count = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
        ).fetchone()[0]
        if count == 0:
            raise RuntimeError(f"Source file contains no data rows: {filename}")
        print(f"EBRASIL_DW_SOURCE_OK file={filename} rows={count}", flush=True)


def create_dimensions(conn: psycopg.Connection) -> None:
    conn.execute("DROP SCHEMA IF EXISTS dw CASCADE")
    conn.execute("CREATE SCHEMA dw")
    for statement in DDL:
        conn.execute(statement)

    conn.execute(
        """INSERT INTO dw.dim_geography
           (zip_code_prefix, city, state, latitude, longitude)
           SELECT geolocation_zip_code_prefix,
                  coalesce(max(nullif(geolocation_city, '')), 'Unknown'),\n                  coalesce(max(nullif(geolocation_state, '')), 'XX'),
                  avg(NULLIF(geolocation_lat, '')::double precision),
                  avg(NULLIF(geolocation_lng, '')::double precision)
             FROM stg_geolocation
            WHERE NULLIF(geolocation_zip_code_prefix, '') IS NOT NULL
            GROUP BY geolocation_zip_code_prefix"""
    )
    conn.execute(
        """INSERT INTO dw.dim_geography
           (zip_code_prefix, city, state, latitude, longitude)
           SELECT prefixes.zip_code_prefix, 'Unknown', 'XX', NULL, NULL
             FROM (
                   SELECT customer_zip_code_prefix AS zip_code_prefix FROM stg_customers
                   UNION
                   SELECT seller_zip_code_prefix AS zip_code_prefix FROM stg_sellers
             ) prefixes
            WHERE NULLIF(prefixes.zip_code_prefix, '') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM dw.dim_geography g
                   WHERE g.zip_code_prefix = prefixes.zip_code_prefix
              )"""
    )
    conn.execute(
        """INSERT INTO dw.dim_geography
           (zip_code_prefix, city, state, latitude, longitude)
           VALUES ('UNKNOWN', 'Unknown', 'XX', NULL, NULL)
           ON CONFLICT (zip_code_prefix) DO NOTHING"""
    )
    conn.execute(
        """INSERT INTO dw.dim_customer
           (customer_unique_id, representative_customer_id, zip_code_prefix, city, state)
           SELECT DISTINCT ON (c.customer_unique_id)
                  c.customer_unique_id,
                  c.customer_id,
                  coalesce(nullif(c.customer_zip_code_prefix, ''), 'UNKNOWN'),
                  coalesce(nullif(c.customer_city, ''), 'Unknown'),
                  coalesce(nullif(c.customer_state, ''), 'XX')
             FROM stg_customers c
            WHERE nullif(c.customer_unique_id, '') IS NOT NULL
            ORDER BY c.customer_unique_id, c.customer_id"""
    )
    conn.execute(
        """INSERT INTO dw.dim_product
           (product_id, category_name, category_name_english, name_length,
            description_length, photos_qty, weight_g, length_cm, height_cm, width_cm)
           SELECT p.product_id,
                  coalesce(nullif(p.product_category_name, ''), 'unknown'),
                  t.product_category_name_english,
                  nullif(p.product_name_lenght, '')::integer,
                  nullif(p.product_description_lenght, '')::integer,
                  nullif(p.product_photos_qty, '')::integer,
                  nullif(p.product_weight_g, '')::numeric,
                  nullif(p.product_length_cm, '')::numeric,
                  nullif(p.product_height_cm, '')::numeric,
                  nullif(p.product_width_cm, '')::numeric
             FROM stg_products p
             LEFT JOIN stg_translation t
               ON t.product_category_name = p.product_category_name
            WHERE nullif(p.product_id, '') IS NOT NULL"""
    )
    conn.execute(
        """INSERT INTO dw.dim_seller (seller_id, zip_code_prefix, city, state)
           SELECT DISTINCT ON (s.seller_id)
                  s.seller_id,
                  coalesce(nullif(s.seller_zip_code_prefix, ''), 'UNKNOWN'),
                  coalesce(nullif(s.seller_city, ''), 'Unknown'),
                  coalesce(nullif(s.seller_state, ''), 'XX')
             FROM stg_sellers s
            WHERE nullif(s.seller_id, '') IS NOT NULL
            ORDER BY s.seller_id"""
    )
    conn.execute(
        """INSERT INTO dw.dim_order_status(order_status)
           SELECT DISTINCT coalesce(nullif(order_status, ''), 'unknown')
             FROM stg_orders"""
    )
    conn.execute(
        """INSERT INTO dw.dim_order_status(order_status)
           VALUES ('unknown') ON CONFLICT DO NOTHING"""
    )
    conn.execute(
        """INSERT INTO dw.dim_payment_type(payment_type)
           SELECT DISTINCT coalesce(nullif(payment_type, ''), 'not_defined')
             FROM stg_payments"""
    )
    conn.execute(
        """INSERT INTO dw.dim_payment_type(payment_type)
           VALUES ('not_defined') ON CONFLICT DO NOTHING"""
    )
    conn.execute(
        """INSERT INTO dw.dim_review_score(review_score, score_label)
           SELECT score, CASE WHEN score = 0 THEN 'unknown' ELSE 'score_' || score::text END
             FROM generate_series(0, 5) AS score"""
    )

    timestamp_columns = (
        ("stg_orders", "order_purchase_timestamp"),
        ("stg_orders", "order_approved_at"),
        ("stg_orders", "order_delivered_carrier_date"),
        ("stg_orders", "order_delivered_customer_date"),
        ("stg_orders", "order_estimated_delivery_date"),
        ("stg_items", "shipping_limit_date"),
        ("stg_reviews", "review_creation_date"),
        ("stg_reviews", "review_answer_timestamp"),
    )
    union = "\nUNION ALL\n".join(
        f"SELECT {ts(column)} AS stamp FROM {table}" for table, column in timestamp_columns
    )
    conn.execute(
        f"""WITH stamps AS ({union}), dates AS (
                 SELECT DISTINCT stamp::date AS calendar_date
                   FROM stamps WHERE stamp IS NOT NULL
             )
             INSERT INTO dw.dim_date
                 (date_key, calendar_date, year, semester, quarter, month,
                  month_name, iso_week, day, weekday, is_weekend)
             SELECT to_char(calendar_date, 'YYYYMMDD')::integer,
                    calendar_date,
                    extract(year FROM calendar_date)::smallint,
                    CASE WHEN extract(month FROM calendar_date) <= 6 THEN 1 ELSE 2 END,
                    extract(quarter FROM calendar_date)::smallint,
                    extract(month FROM calendar_date)::smallint,
                    to_char(calendar_date, 'FMMonth'),
                    extract(week FROM calendar_date)::smallint,
                    extract(day FROM calendar_date)::smallint,
                    extract(isodow FROM calendar_date)::smallint,
                    extract(isodow FROM calendar_date) IN (6, 7)
               FROM dates"""
    )
    conn.execute(
        """INSERT INTO dw.dim_time(time_key, hour, day_part)
           SELECT hour * 10000, hour,
                  CASE WHEN hour < 6 THEN 'night'
                       WHEN hour < 12 THEN 'morning'
                       WHEN hour < 18 THEN 'afternoon'
                       ELSE 'evening' END
             FROM generate_series(0, 23) AS hour"""
    )


def create_stars(conn: psycopg.Connection) -> None:
    p = ts("o.order_purchase_timestamp")
    approved = ts("o.order_approved_at")
    shipped = ts("o.order_delivered_carrier_date")
    delivered = ts("o.order_delivered_customer_date")
    estimate = ts("o.order_estimated_delivery_date")

    conn.execute(
        f"""INSERT INTO dw.fact_order_item
            (order_id, order_item_id, customer_unique_id, product_id, seller_id,
             order_status, customer_zip_code_prefix, seller_zip_code_prefix,
             purchase_date_key, purchase_time_key, shipping_limit_date_key,
             shipping_limit_time_key, price, freight_value, line_total, gmv,
             item_count, event_ts)
            SELECT i.order_id, nullif(i.order_item_id, '')::integer,
                   c.customer_unique_id, i.product_id, i.seller_id,
                   coalesce(nullif(o.order_status, ''), 'unknown'),
                   coalesce(nullif(c.customer_zip_code_prefix, ''), 'UNKNOWN'),
                   s.zip_code_prefix,
                   {date_key(p)}, {time_key(p)},
                   {date_key(ts('i.shipping_limit_date'))},
                   {time_key(ts('i.shipping_limit_date'))},
                   nullif(i.price, '')::numeric,
                   nullif(i.freight_value, '')::numeric,
                   nullif(i.price, '')::numeric + nullif(i.freight_value, '')::numeric,
                   nullif(i.price, '')::numeric,
                   1, {p}
              FROM stg_items i
              JOIN stg_orders o ON o.order_id = i.order_id
              JOIN stg_customers c ON c.customer_id = o.customer_id
              JOIN dw.dim_seller s ON s.seller_id = i.seller_id
             WHERE nullif(i.order_item_id, '') IS NOT NULL"""
    )

    conn.execute(
        f"""WITH item_totals AS (
                 SELECT order_id, count(*)::bigint AS item_count,
                        count(DISTINCT product_id)::bigint AS product_count,
                        count(DISTINCT seller_id)::bigint AS seller_count,
                        sum(nullif(price, '')::numeric) AS product_amount,
                        sum(nullif(freight_value, '')::numeric) AS freight_amount
                   FROM stg_items GROUP BY order_id
             ), payment_totals AS (
                 SELECT order_id, sum(nullif(payment_value, '')::numeric) AS payment_amount
                   FROM stg_payments GROUP BY order_id
             )
             INSERT INTO dw.fact_order
                 (order_id, customer_unique_id, order_status,
                  purchase_date_key, purchase_time_key,
                  approved_date_key, approved_time_key,
                  shipped_date_key, shipped_time_key,
                  delivered_date_key, delivered_time_key,
                  estimated_delivery_date_key, estimated_delivery_time_key,
                  item_count, product_count, seller_count, product_amount,
                  freight_amount, payment_amount, payment_gap, event_ts)
             SELECT o.order_id, c.customer_unique_id,
                    coalesce(nullif(o.order_status, ''), 'unknown'),
                    {date_key(p)}, {time_key(p)},
                    {date_key(approved)}, {time_key(approved)},
                    {date_key(shipped)}, {time_key(shipped)},
                    {date_key(delivered)}, {time_key(delivered)},
                    {date_key(estimate)}, {time_key(estimate)},
                    coalesce(i.item_count, 0), coalesce(i.product_count, 0),
                    coalesce(i.seller_count, 0), coalesce(i.product_amount, 0),
                    coalesce(i.freight_amount, 0), coalesce(pay.payment_amount, 0),
                    coalesce(pay.payment_amount, 0) - coalesce(i.product_amount, 0)
                        - coalesce(i.freight_amount, 0),
                    {p}
               FROM stg_orders o
               JOIN stg_customers c ON c.customer_id = o.customer_id
               LEFT JOIN item_totals i ON i.order_id = o.order_id
               LEFT JOIN payment_totals pay ON pay.order_id = o.order_id"""
    )

    conn.execute(
        f"""INSERT INTO dw.fact_payment
            (order_id, payment_sequential, customer_unique_id, payment_type,
             purchase_date_key, purchase_time_key, payment_value, installments,
             transaction_count, event_ts)
            SELECT pmt.order_id,
                   coalesce(nullif(pmt.payment_sequential, '')::integer, 1),
                   c.customer_unique_id,
                   coalesce(nullif(pmt.payment_type, ''), 'not_defined'),
                   {date_key(p)}, {time_key(p)},
                   coalesce(nullif(pmt.payment_value, '')::numeric, 0),
                   coalesce(nullif(pmt.payment_installments, '')::integer, 0),
                   1, {p}
              FROM stg_payments pmt
              JOIN stg_orders o ON o.order_id = pmt.order_id
              JOIN stg_customers c ON c.customer_id = o.customer_id"""
    )

    first_seller = """WITH seller_order AS (
        SELECT DISTINCT ON (order_id) order_id, seller_id
          FROM stg_items ORDER BY order_id, nullif(order_item_id, '')::integer
    )"""
    conn.execute(
        f"""{first_seller}
            INSERT INTO dw.fact_logistics
                (order_id, customer_unique_id, representative_seller_id,
                 customer_zip_code_prefix, seller_zip_code_prefix, order_status,
                 purchase_date_key, purchase_time_key,
                 approved_date_key, approved_time_key,
                 shipped_date_key, shipped_time_key,
                 delivered_date_key, delivered_time_key,
                 estimated_delivery_date_key, estimated_delivery_time_key,
                 distance_km, delivery_days, late_days, delivered_on_time,
                 orders_count, event_ts)
            SELECT o.order_id, c.customer_unique_id, so.seller_id,
                   coalesce(nullif(c.customer_zip_code_prefix, ''), 'UNKNOWN'),
                   s.zip_code_prefix,
                   coalesce(nullif(o.order_status, ''), 'unknown'),
                   {date_key(p)}, {time_key(p)},
                   {date_key(approved)}, {time_key(approved)},
                   {date_key(shipped)}, {time_key(shipped)},
                   {date_key(delivered)}, {time_key(delivered)},
                   {date_key(estimate)}, {time_key(estimate)},
                   CASE WHEN cg.latitude IS NULL OR sg.latitude IS NULL
                              OR cg.longitude IS NULL OR sg.longitude IS NULL
                        THEN NULL
                        ELSE 6371.0088 * 2 * asin(sqrt(least(1.0, greatest(0.0,
                            power(sin(radians(sg.latitude - cg.latitude) / 2), 2)
                            + cos(radians(cg.latitude)) * cos(radians(sg.latitude))
                            * power(sin(radians(sg.longitude - cg.longitude) / 2), 2)))))
                   END,
                   CASE WHEN {delivered} IS NULL THEN NULL
                        ELSE extract(epoch FROM ({delivered} - {p})) / 86400 END,
                   CASE WHEN {delivered} IS NULL OR {estimate} IS NULL THEN NULL
                        ELSE extract(epoch FROM ({delivered} - {estimate})) / 86400 END,
                   CASE WHEN {delivered} IS NOT NULL AND {estimate} IS NOT NULL
                              AND {delivered} <= {estimate} THEN 1 ELSE 0 END,
                   1, {p}
              FROM stg_orders o
              JOIN stg_customers c ON c.customer_id = o.customer_id
              LEFT JOIN seller_order so ON so.order_id = o.order_id
              LEFT JOIN dw.dim_seller s ON s.seller_id = so.seller_id
              LEFT JOIN dw.dim_geography cg
                     ON cg.zip_code_prefix = coalesce(nullif(c.customer_zip_code_prefix, ''), 'UNKNOWN')
              LEFT JOIN dw.dim_geography sg
                     ON sg.zip_code_prefix = s.zip_code_prefix"""
    )

    review_created = ts("r.review_creation_date")
    review_answer = ts("r.review_answer_timestamp")
    conn.execute(
        f"""INSERT INTO dw.fact_review
            (review_id, order_id, customer_unique_id, order_status, review_score,
             creation_date_key, creation_time_key, answer_date_key, answer_time_key,
             review_count, score_sum, has_comment, response_hours, event_ts)
            SELECT r.review_id, r.order_id, c.customer_unique_id,
                   coalesce(nullif(o.order_status, ''), 'unknown'),
                   coalesce(nullif(r.review_score, '')::smallint, 0),
                   {date_key(review_created)}, {time_key(review_created)},
                   {date_key(review_answer)}, {time_key(review_answer)},
                   1, coalesce(nullif(r.review_score, '')::integer, 0),
                   nullif(concat_ws(' ', nullif(r.review_comment_title, ''),
                                         nullif(r.review_comment_message, '')), '') IS NOT NULL,
                   CASE WHEN {review_created} IS NULL OR {review_answer} IS NULL THEN NULL
                        ELSE extract(epoch FROM ({review_answer} - {review_created})) / 3600 END,
                   coalesce({review_created}, {p})
              FROM stg_reviews r
              JOIN stg_orders o ON o.order_id = r.order_id
              JOIN stg_customers c ON c.customer_id = o.customer_id"""
    )

    latest = "(SELECT max(date_trunc('hour', NULLIF(order_purchase_timestamp, '')::timestamp)) FROM stg_orders)"
    conn.execute(
        f"""WITH customer_spend AS (
                 SELECT c.customer_unique_id,
                        max({p}) AS last_purchase,
                        count(DISTINCT o.order_id)::bigint AS frequency,
                        coalesce(sum(nullif(i.price, '')::numeric
                                     + nullif(i.freight_value, '')::numeric), 0) AS monetary
                   FROM stg_orders o
                   JOIN stg_customers c ON c.customer_id = o.customer_id
                   LEFT JOIN stg_items i ON i.order_id = o.order_id
                  GROUP BY c.customer_unique_id
             )
             INSERT INTO dw.mart_customer_rfm
                 (customer_unique_id, last_purchase_date_key, last_purchase_time_key,
                  frequency, monetary, recency_days, average_order_value)
             SELECT customer_unique_id,
                    {date_key('last_purchase')}, {time_key('last_purchase')},
                    frequency, monetary,
                    extract(epoch FROM ({latest} - last_purchase)) / 86400,
                    CASE WHEN frequency = 0 THEN 0 ELSE monetary / frequency END
               FROM customer_spend"""
    )


def main() -> None:
    for filename, _columns in SOURCES.values():
        if not (DATA_DIR / filename).is_file():
            raise FileNotFoundError(f"Required eBrasil source file is missing: {DATA_DIR / filename}")

    ensure_database()
    target = psycopg.connect(dbname=DATABASE, **PG)
    try:
        with target.transaction():
            target.execute("SET TIME ZONE 'UTC'")
            stage_sources(target)
            create_dimensions(target)
            create_stars(target)
            for table in ("dim_customer", "dim_product", "dim_seller",
                          "dim_date", "dim_time", "dim_geography",
                          "dim_order_status", "dim_payment_type",
                          "dim_review_score", "fact_order_item", "fact_order",
                          "fact_payment", "fact_logistics", "fact_review",
                          "mart_customer_rfm"):
                count = target.execute(
                    sql.SQL("SELECT count(*) FROM dw.{}").format(sql.Identifier(table))
                ).fetchone()[0]
                if count == 0:
                    raise RuntimeError(f"Warehouse table is empty: dw.{table}")
                print(f"EBRASIL_DW_TABLE_CREATED table=dw.{table} rows={count}", flush=True)
            target.execute("ANALYZE dw.dim_customer")
            target.execute("ANALYZE dw.dim_product")
            target.execute("ANALYZE dw.dim_seller")
            target.execute("ANALYZE dw.fact_order_item")
            target.execute("ANALYZE dw.fact_order")
            target.execute("ANALYZE dw.fact_payment")
            target.execute("ANALYZE dw.fact_logistics")
            target.execute("ANALYZE dw.fact_review")
            target.execute("ANALYZE dw.mart_customer_rfm")
            foreign_keys = target.execute(
                """SELECT count(*) FROM pg_constraint c
                    JOIN pg_namespace n ON n.oid = c.connamespace
                   WHERE n.nspname = 'dw' AND c.contype = 'f'"""
            ).fetchone()[0]
            if foreign_keys < 53:
                raise RuntimeError(f"Expected at least 53 dimension foreign keys, found {foreign_keys}")
            print(f"EBRASIL_DW_BUILD_OK database={DATABASE} tables=15 foreign_keys={foreign_keys}", flush=True)
    finally:
        target.close()


if __name__ == "__main__":
    main()
