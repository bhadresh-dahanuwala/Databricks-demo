import dlt
from pyspark.sql.functions import col, current_date, to_date

@dlt.table(
    name="order__postgres",
    comment="Raw order data ingested from Supabase Postgres via Lakehouse Federation.",
    table_properties={"quality": "bronze"}
)
def raw_order_postgres():
    return (
        spark.read.table("supabase.public.orders")
        .withColumn("source_date", to_date(col("order_timestamp")))
    )

@dlt.table(
    name="order_item__postgres",
    comment="Raw order item data ingested from Supabase Postgres via Lakehouse Federation.",
    table_properties={"quality": "bronze"}
)
def raw_order_item_postgres():
    return (
        spark.read.table("supabase.public.order_items")
        .withColumn("source_date", current_date())
    )
