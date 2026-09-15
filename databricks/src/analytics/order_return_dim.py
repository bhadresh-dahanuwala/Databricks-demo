from pyspark import pipelines as dp

@dp.temporary_view(name="order_return_cdc")
def order_return_cdc():
    return (
        spark.readStream.table("ecomm.staging.returns")
        .select("order_id", "return_reason", "source_date")
    )

dp.create_streaming_table(
    name="order_return_dim",
    comment="Captures fixed context about the return event.",
    schema="""
        order_return_key  BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id          INT,
        return_reason     STRING,
        source_date       DATE
    """
)

dp.create_auto_cdc_flow(
    target="order_return_dim",
    source="order_return_cdc",
    keys=["order_id"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
