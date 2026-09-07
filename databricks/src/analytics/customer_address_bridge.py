import dlt

@dlt.view
def customer_address_cdc():
    df_customer_address = spark.readStream.table("ecomm.staging.customer_address")
    # Read address_dim as a static table (batch) for lookup
    df_address_dim = spark.table("ecomm.analytics.address_dim")
    
    return df_customer_address.join(
        df_address_dim,
        on=["line_1", "line_2", "zip"],
        how="inner"
    ).select(
        df_customer_address["customer_id"],
        df_address_dim["address_key"],
        df_customer_address["address_type"],
        df_customer_address["source_date"]
    )

dlt.create_streaming_table(
    name="customer_address_bridge",
    comment="Historized bridge resolving the many-to-many relationship between customers and addresses.",
    schema="""
        customer_address_key BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id          INT,
        address_type         STRING,
        address_key          BIGINT,
        source_date          DATE,
        __START_AT           DATE,
        __END_AT             DATE,
        __ACTIVE             BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

dlt.apply_changes(
    target="customer_address_bridge",
    source="customer_address_cdc",
    keys=["customer_id", "address_type"],
    sequence_by="source_date",
    stored_as_scd_type=2
)