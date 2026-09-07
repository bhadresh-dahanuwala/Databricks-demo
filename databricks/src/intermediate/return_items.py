import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="return_items",
    comment="Enriched return line items with header-level timestamps and reasons."
)
def intermediate_return_items():
    return_item = spark.table("ecomm.staging.return_item")
    returns = spark.table("ecomm.staging.returns")
    
    return (
        return_item.alias("ri")
        .join(returns.alias("r"), col("ri.order_id") == col("r.order_id"), "inner")
        .select(
            col("ri.order_id"),
            col("ri.product_id"),
            col("r.return_request_timestamp"),
            col("r.return_reason"),
            col("ri.quantity"),
            col("ri.quantity_received")
        )
    )
