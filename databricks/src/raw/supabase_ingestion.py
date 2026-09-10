# Databricks notebook source
# Ingest raw orders and order_items from Supabase PostgreSQL via Lakehouse Federation into managed Delta tables
from pyspark.sql.functions import col, current_date, to_date

(
    spark.read.table("supabase.public.orders")
    .withColumn("source_date", to_date(col("order_timestamp")))
    .write.format("delta").mode("overwrite").saveAsTable("ecomm.raw.order__postgres")
)

(
    spark.read.table("supabase.public.order_items")
    .withColumn("source_date", current_date())
    .write.format("delta").mode("overwrite").saveAsTable("ecomm.raw.order_item__postgres")
)
