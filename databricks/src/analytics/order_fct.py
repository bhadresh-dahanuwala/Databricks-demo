import dlt
from pyspark.sql.functions import col, sum as _sum, first

@dlt.table(
    name="order_fct",
    comment="Fact table for order level aggregations.",
    schema="""
        order_id                INT,
        order_key               BIGINT,
        order_status_key        BIGINT,
        customer_key            BIGINT,
        customer_address_key    BIGINT,
        order_date_key          INT,
        order_cost_amount       FLOAT,
        order_gross_amount      FLOAT,
        order_net_amount        FLOAT
    """
)
def order_fct():
    df_order_item = spark.table("ecomm.analytics.order_item_fct")
    df_prod_fct = spark.table("ecomm.analytics.product_fct").filter(col("__ACTIVE") == True)
    
    joined = df_order_item.join(df_prod_fct, on="product_id", how="left")
    
    return joined.groupBy("order_id").agg(
        first("order_key").alias("order_key"),
        first("order_status_key").alias("order_status_key"),
        first("customer_key").alias("customer_key"),
        first("customer_address_key").alias("customer_address_key"),
        first("order_date_key").alias("order_date_key"),
        _sum(col("quantity") * col("unit_cost")).cast("float").alias("order_cost_amount"),
        _sum("gross_amount").cast("float").alias("order_gross_amount"),
        _sum("net_amount").cast("float").alias("order_net_amount")
    )
