import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="order_items",
    comment="Enriched order line items with pre-calculated financial measures."
)
def intermediate_order_items():
    order_item = dlt.read("order_item")
    order = dlt.read("order")
    product = dlt.read("product")
    
    return (
        order_item.alias("oi")
        .join(order.alias("o"), col("oi.order_id") == col("o.order_id"), "inner")
        .join(product.alias("p"), col("oi.product_id") == col("p.id"), "inner")
        .select(
            col("oi.order_id"),
            col("oi.product_id"),
            col("o.customer_id"),
            col("o.order_timestamp"),
            col("o.shipping_label"),
            col("oi.quantity"),
            col("p.price").alias("unit_price"),
            col("o.discount_percentage"),
            (col("oi.quantity") * col("p.price")).alias("gross_amount"),
            ((col("oi.quantity") * col("p.price")) * (1 - col("o.discount_percentage") / 100)).alias("net_amount")
        )
    )
