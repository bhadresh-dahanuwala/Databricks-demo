import dlt

@dlt.view
def order_status_cdc():
    return (
        spark.readStream.table("ecomm.staging.order")
        .select(
            "order_id",
            "order_status",
            "source_date"
        )
    )

dlt.create_streaming_table(
    name="order_status_dim",
    comment="Order status transitions over time (SCD Type 2).",
    schema="""
        order_status_key    BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id            INT,
        order_status        STRING,
        __START_AT          DATE,
        __END_AT            DATE,
        __ACTIVE            BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

dlt.apply_changes(
    target="order_status_dim",
    source="order_status_cdc",
    keys=["order_id"],
    sequence_by="source_date",
    stored_as_scd_type=2
)
