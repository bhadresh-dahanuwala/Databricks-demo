import dlt
from pyspark.sql.functions import col, datediff, to_date, when, lit

@dlt.table(
    name="order_items",
    comment="Enriched order line items with pre-calculated financial measures."
)
def intermediate_order_items():
    order_item = spark.readStream.option("ignoreChanges", "true").table("ecomm.staging.order_item")
    order = spark.table("ecomm.staging.order")
    product = spark.table("ecomm.staging.product")
    
    return (
        order_item.alias("oi")
        .join(order.alias("o"), col("oi.order_id") == col("o.order_id"), "inner")
        .join(product.alias("p"), col("oi.product_id") == col("p.product_id"), "inner")
        .select(
            col("oi.order_id"),
            col("oi.product_id"),
            col("o.customer_id"),
            col("o.order_timestamp"),
            col("o.shipping_label"),
            col("oi.quantity"),
            col("p.unit_price").alias("unit_price"),
            col("p.unit_cost").alias("unit_cost"),
            col("o.discount_percentage"),
            (col("oi.quantity") * col("p.unit_cost")).cast("float").alias("cost_amount"),
            (col("oi.quantity") * col("p.unit_price")).cast("float").alias("gross_amount"),
            ((col("oi.quantity") * col("p.unit_price")) * (1 - col("o.discount_percentage") / 100)).cast("float").alias("net_amount"),
            when(col("o.shipping_label").isNotNull(), datediff(col("oi.source_date"), to_date(col("o.order_timestamp")))).otherwise(lit(None).cast("int")).alias("days_to_ship"),
            col("oi.source_date")
        )
    )
