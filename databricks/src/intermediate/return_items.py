import dlt
from pyspark.sql.functions import col, when, lit

@dlt.table(
    name="return_items",
    comment="Enriched return line items with header-level timestamps and reasons."
)
def intermediate_return_items():
    return_item = spark.readStream.table("ecomm.staging.return_item")
    returns = spark.table("ecomm.staging.returns")
    order = spark.table("ecomm.staging.order")
    order_item = spark.table("ecomm.staging.order_item")
    product = spark.table("ecomm.staging.product")
    
    return (
        return_item.alias("ri")
        .join(returns.alias("r"), col("ri.order_id") == col("r.order_id"), "inner")
        .join(order_item.alias("oi"), (col("ri.order_id") == col("oi.order_id")) & (col("ri.product_id") == col("oi.product_id")), "left")
        .join(order.alias("o"), col("ri.order_id") == col("o.order_id"), "left")
        .join(product.alias("p"), col("ri.product_id") == col("p.id"), "left")
        .select(
            col("ri.order_id"),
            col("ri.product_id"),
            col("r.return_request_timestamp"),
            col("r.return_reason"),
            col("ri.quantity_received"),
            (col("ri.quantity_received") * col("p.cost")).alias("return_cost_amount"),
            when(col("oi.quantity") > 0, ((col("oi.quantity") * col("p.price")) * (1 - col("o.discount_percentage") / 100) / col("oi.quantity")) * col("ri.quantity_received")).otherwise(lit(0.0)).alias("return_amount"),
            col("ri.source_date")
        )
    )
