import dlt

@dlt.view
def customer_cdc():
    return spark.readStream.table("ecomm.staging.customer")

dlt.create_streaming_table(
    name="customer_dim",
    comment="",
    schema="""
        customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id INT,
        first_name STRING,
        last_name STRING,
        source_date DATE,
        __START_AT DATE,
        __END_AT DATE,
        __ACTIVE BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

dlt.apply_changes(
    target="customer_dim",
    source="customer_cdc",
    keys=["customer_id"],
    sequence_by="source_date",
    stored_as_scd_type=2
)
