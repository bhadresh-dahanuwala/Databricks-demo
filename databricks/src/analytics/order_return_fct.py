import dlt
from pyspark.sql.functions import col, sum as _sum, first

@dlt.table(
    name="order_return_fct",
    comment="Fact table for order return level aggregations.",
    schema="""
        order_id                  INT,
        order_return_key          BIGINT,
        return_request_date_key   INT,
        return_received_date_key  INT,
        order_key                 BIGINT,
        order_status_key          BIGINT,
        customer_key              BIGINT,
        customer_address_key      BIGINT,
        return_cost_amount        FLOAT,
        return_amount             FLOAT
    """
)
def order_return_fct():
    df_return_item_fct = spark.table("ecomm.analytics.return_item_fct")
    
    return df_return_item_fct.groupBy("order_id").agg(
        first("order_return_key").alias("order_return_key"),
        first("return_request_date_key").alias("return_request_date_key"),
        first("return_received_date_key").alias("return_received_date_key"),
        first("order_key").alias("order_key"),
        first("order_status_key").alias("order_status_key"),
        first("customer_key").alias("customer_key"),
        first("customer_address_key").alias("customer_address_key"),
        _sum("return_cost_amount").alias("return_cost_amount"),
        _sum("return_amount").alias("return_amount")
    )
