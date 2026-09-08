import dlt
from pyspark.sql.functions import col, date_format, datediff, coalesce, lit, to_date, when

@dlt.view
def order_item_cdc():
    df_item = spark.readStream.table("ecomm.staging.order_item")
    df_order = spark.table("ecomm.staging.order")
    df_order_dim = spark.table("ecomm.analytics.order_dim")
    df_prod_dim = spark.table("ecomm.analytics.product_dim")
    df_cust_dim = spark.table("ecomm.analytics.customer_dim").filter(col("__ACTIVE") == True)
    df_prod_fct = spark.table("ecomm.analytics.product_fct").filter(col("__ACTIVE") == True)
    df_order_status = spark.table("ecomm.analytics.order_status_dim").filter(col("__ACTIVE") == True)
    df_cust_addr = spark.table("ecomm.analytics.customer_address_bridge").filter((col("__ACTIVE") == True) & (col("address_type") == "Home"))

    joined = df_item.join(df_order, on="order_id", how="left") \
        .join(df_order_dim, on="order_id", how="left") \
        .join(df_prod_dim, on="product_id", how="left") \
        .join(df_cust_dim, on="customer_id", how="left") \
        .join(df_prod_fct, on="product_id", how="left") \
        .join(df_order_status, on="order_id", how="left") \
        .join(df_cust_addr, on="customer_id", how="left")

    return joined.select(
        df_item["order_id"],
        df_item["product_id"],
        df_order_dim["order_key"],
        df_prod_dim["product_key"],
        df_order_status["order_status_key"],
        df_cust_dim["customer_key"],
        df_cust_addr["customer_address_key"],
        date_format(df_order["order_timestamp"], "yyyyMMdd").cast("int").alias("order_date_key"),
        df_order["shipping_label"],
        df_item["quantity"],
        (df_item["quantity"] * df_prod_fct["unit_price"]).alias("gross_amount"),
        ((df_item["quantity"] * df_prod_fct["unit_price"]) * (1 - coalesce(df_order["discount_percentage"], lit(0.0)) / 100)).alias("net_amount"),
        when(df_order["shipping_label"].isNotNull(), datediff(df_item["source_date"], to_date(df_order["order_timestamp"]))).otherwise(lit(None).cast("int")).alias("days_to_ship"),
        df_item["source_date"]
    )

dlt.create_streaming_table(
    name="order_item_fct",
    comment="Accumulating snapshot fact table for order line items.",
    schema="""
        order_item_key          BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id                INT,
        product_id              INT,
        order_key               BIGINT,
        product_key             BIGINT,
        order_status_key        BIGINT,
        customer_key            BIGINT,
        customer_address_key    BIGINT,
        order_date_key          INT,
        shipping_label          STRING,
        quantity                INT,
        gross_amount            FLOAT,
        net_amount              FLOAT,
        days_to_ship            INT
    """
)

dlt.apply_changes(
    target="order_item_fct",
    source="order_item_cdc",
    keys=["order_id", "product_id"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
