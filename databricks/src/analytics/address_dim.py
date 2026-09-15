from pyspark import pipelines as dp

@dp.temporary_view(name="address_cdc")
def address_cdc():
    return (
        spark.readStream.table("ecomm.staging.customer_address")
        .select("line_1", "line_2", "city", "state", "zip", "source_date")
    )

dp.create_streaming_table(
    name="address_dim",
    comment="",
    schema="""
        address_key     BIGINT GENERATED ALWAYS AS IDENTITY,
        line_1          STRING,
        line_2          STRING,
        city            STRING,
        state           STRING,
        zip             STRING,
        source_date     DATE
    """
)

dp.create_auto_cdc_flow(
    target="address_dim",
    source="address_cdc",
    keys=["line_1", "line_2", "zip"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
