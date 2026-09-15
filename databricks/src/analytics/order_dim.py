from pyspark import pipelines as dp

@dp.temporary_view(name="order_dim_cdc")
def order_dim_cdc():
    return (
        spark.readStream.option("ignoreChanges", "true").table("ecomm.staging.order")
        .select(
            "order_id",
            "order_timestamp",
            "customer_id",
            "order_mode",
            "return_windows_days",
            "discount_percentage",
            "source_date"
        )
    )

dp.create_streaming_table(
    name="order_dim",
    comment="Dimension containing stable attributes of an order.",
    schema="""
        order_key           BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id            INT,
        order_timestamp     TIMESTAMP,
        customer_id         INT,
        order_mode          STRING,
        return_windows_days INT,
        discount_percentage FLOAT,
        source_date         DATE
    """
)

dp.create_auto_cdc_flow(
    target="order_dim",
    source="order_dim_cdc",
    keys=["order_id"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
