import dlt
from pyspark.sql.functions import col, current_date, to_date

@dlt.table(
    name="order",
    comment="Raw order data ingested from Supabase Postgres via Lakehouse Federation."
)
def raw_order():
    return (
        spark.read.table("supabase.public.orders")
        .withColumn("source_date", to_date(col("order_timestamp")))
    )

@dlt.table(
    name="order_item",
    comment="Raw order item data ingested from Supabase Postgres via Lakehouse Federation."
)
def raw_order_item():
    return (
        spark.read.table("supabase.public.order_items")
        .withColumn("source_date", current_date())
    )
