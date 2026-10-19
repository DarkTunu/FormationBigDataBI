"""Validate the eBrasilDW database created by build_ebrasil_dw.py."""
from __future__ import annotations

import os
import sys

import psycopg
from psycopg import sql


DATABASE = os.getenv("EBRASIL_DW_DATABASE", "ebrasilDW")
PG = {
    "host": os.getenv("PGHOST", "postgres-source"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "formation"),
    "password": os.environ["PGPASSWORD"],
}
TABLES = (
    "dim_geography", "dim_customer", "dim_product", "dim_seller",
    "dim_order_status", "dim_payment_type", "dim_review_score",
    "dim_date", "dim_time", "fact_order_item", "fact_order",
    "fact_payment", "fact_logistics", "fact_review",
)
FACTS = (
    "fact_order_item", "fact_order", "fact_payment", "fact_logistics",
    "fact_review",
)


def main() -> None:
    with psycopg.connect(dbname=DATABASE, **PG) as conn:
        missing = [name for name in TABLES
                   if conn.execute("SELECT to_regclass(%s)", (f"dw.{name}",)).fetchone()[0] is None]
        if missing:
            raise RuntimeError(f"Missing eBrasilDW tables: {missing}")

        empty = []
        for name in TABLES:
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM dw.{}").format(sql.Identifier(name))
            ).fetchone()[0]
            print(f"EBRASIL_DW_CHECK_TABLE table=dw.{name} rows={count}", flush=True)
            if count <= 0:
                empty.append(name)
        if empty:
            raise RuntimeError(f"Empty eBrasilDW tables: {empty}")

        fk_count = conn.execute(
            """SELECT count(*) FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
               WHERE n.nspname = 'dw' AND c.contype = 'f'"""
        ).fetchone()[0]
        if fk_count < 53:
            raise RuntimeError(f"Expected at least 53 dimension foreign keys; found {fk_count}")

        bad_time = {}
        for name in FACTS:
            bad_time[name] = conn.execute(
                sql.SQL("SELECT count(*) FROM dw.{} WHERE event_ts <> date_trunc('hour', event_ts)")
                   .format(sql.Identifier(name))
            ).fetchone()[0]
        invalid = {name: count for name, count in bad_time.items() if count}
        if invalid:
            raise RuntimeError(f"Event timestamps contain minutes/seconds: {invalid}")

        hour_count, min_hour, max_hour = conn.execute(
            "SELECT count(*), min(hour), max(hour) FROM dw.dim_time"
        ).fetchone()
        if (hour_count, min_hour, max_hour) != (24, 0, 23):
            raise RuntimeError(
                f"Expected 24 hourly values (0..23) in dw.dim_time, got "
                f"{hour_count} ({min_hour}..{max_hour})"
            )
        print(
            f"EBRASIL_DW_CHECK_OK database={DATABASE} tables={len(TABLES)} "
            f"foreign_keys={fk_count} hourly_grain=ok",
            flush=True,
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"EBRASIL_DW_CHECK_FAILED: {exc}", file=sys.stderr, flush=True)
        raise
