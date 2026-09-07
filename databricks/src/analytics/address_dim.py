import dlt

@dlt.view
def address_cdc():
    return spark.readStream.table("ecomm.staging.customer_address")

dlt.create_streaming_table(
    name="address_dim",
    comment="",
    schema="""
        address_key     BIGINT GENERATED ALWAYS AS IDENTITY,
        line_1          STRING,
        line_2          STRING,
        city            STRING,
        state           STRING,
        zip             STRING
    """
)

dlt.apply_changes(
    target="address_dim",
    source="address_cdc",
    keys=["line_1", "line_2", "zip"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
