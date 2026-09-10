import dlt
from pyspark.sql.functions import col, date_format, lit, to_date, when

@dlt.view
def return_item_cdc():
    df_ret_item = spark.readStream.option("ignoreChanges", "true").table("ecomm.intermediate.return_items")
    df_order_ret_dim = spark.table("ecomm.analytics.order_return_dim")
    df_order_item_fct = spark.table("ecomm.analytics.order_item_fct")
    
    joined = df_ret_item.join(df_order_ret_dim, on="order_id", how="left") \
        .join(df_order_item_fct, on=["order_id", "product_id"], how="left")
        
    return joined.select(
        df_ret_item["order_id"],
        df_ret_item["product_id"],
        date_format(df_ret_item["return_request_timestamp"], "yyyyMMdd").cast("int").alias("return_request_date_key"),
        date_format(df_ret_item["source_date"], "yyyyMMdd").cast("int").alias("return_received_date_key"),
        df_order_item_fct["order_key"],
        df_order_item_fct["order_status_key"],
        df_order_ret_dim["order_return_key"],
        df_order_item_fct["product_key"],
        df_order_item_fct["customer_key"],
        df_order_item_fct["customer_address_key"],
        df_ret_item["quantity_received"],
        df_ret_item["return_cost_amount"],
        df_ret_item["return_amount"],
        df_ret_item["source_date"]
    )

dlt.create_streaming_table(
    name="return_item_fct",
    comment="Fact table for return line items.",
    schema="""
        return_item_key           BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id                  INT,
        product_id                INT,
        return_request_date_key   INT,
        return_received_date_key  INT,
        order_key                 BIGINT,
        order_status_key          BIGINT,
        order_return_key          BIGINT,
        product_key               BIGINT,
        customer_key              BIGINT,
        customer_address_key      BIGINT,
        quantity_received         INT,
        return_cost_amount        FLOAT,
        return_amount             FLOAT,
        source_date               DATE
    """
)

dlt.apply_changes(
    target="return_item_fct",
    source="return_item_cdc",
    keys=["order_id", "product_id"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
