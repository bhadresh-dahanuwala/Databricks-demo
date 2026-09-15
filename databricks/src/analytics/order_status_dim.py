from pyspark import pipelines as dp

@dp.temporary_view(name="order_status_cdc")
def order_status_cdc():
    return (
        spark.readStream.option("ignoreChanges", "true").table("ecomm.staging.order")
        .select(
            "order_id",
            "order_status",
            "source_date"
        )
    )

dp.create_streaming_table(
    name="order_status_dim",
    comment="Order status transitions over time (SCD Type 2).",
    schema="""
        order_status_key    BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id            INT,
        order_status        STRING,
        source_date         DATE,
        __START_AT          DATE,
        __END_AT            DATE,
        __ACTIVE            BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

dp.create_auto_cdc_flow(
    target="order_status_dim",
    source="order_status_cdc",
    keys=["order_id"],
    sequence_by="source_date",
    stored_as_scd_type=2
)
