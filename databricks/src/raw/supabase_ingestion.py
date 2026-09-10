# Databricks notebook source
# Ingest raw orders and order_items from Supabase PostgreSQL via Lakehouse Federation into managed Delta tables
from pyspark.sql.functions import col, to_date

def sync_table_incrementally(source_table: str, target_table: str, date_expr_col: str):
    has_table = spark.catalog.tableExists(target_table)
    has_updated_at = False
    if has_table:
        cols = [c.name for c in spark.catalog.listColumns(target_table)]
        has_updated_at = "updated_at" in cols

    if has_updated_at:
        # Incremental extraction: query high watermark
        max_wm = (
            spark.table(target_table)
            .selectExpr("coalesce(max(updated_at), cast('1970-01-01 00:00:00' as timestamp)) as wm")
            .collect()[0]["wm"]
        )
        print(f"[{target_table}] High watermark: {max_wm}")
        df_new = (
            spark.read.table(source_table)
            .filter(col("updated_at") > max_wm)
            .withColumn("source_date", to_date(col(date_expr_col)))
        )
        if df_new.take(1):
            count = df_new.count()
            print(f"[{target_table}] Appending {count} new/updated records")
            df_new.write.format("delta").mode("append").saveAsTable(target_table)
        else:
            print(f"[{target_table}] No new records since {max_wm}")
    else:
        # Initial extraction / schema migration: write with updated_at column
        print(f"[{target_table}] Initial load / schema seeding from {source_table}")
        df_initial = (
            spark.read.table(source_table)
            .withColumn("source_date", to_date(col(date_expr_col)))
        )
        df_initial.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print(f"[{target_table}] Initial load complete ({df_initial.count()} records)")

# 1. Ingest orders incrementally
sync_table_incrementally(
    source_table="supabase.public.orders",
    target_table="ecomm.raw.order__postgres",
    date_expr_col="order_timestamp"
)

# 2. Ingest order_items incrementally
sync_table_incrementally(
    source_table="supabase.public.order_items",
    target_table="ecomm.raw.order_item__postgres",
    date_expr_col="updated_at"
)

