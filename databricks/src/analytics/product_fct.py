import dlt

@dlt.view
def product_fct_cdc():
    return (
        spark.readStream.table("ecomm.staging.product")
        .select(
            "product_id",
            "stock_quantity",
            "unit_cost",
            "unit_price",
            "source_date"
        )
    )

dlt.create_streaming_table(
    name="product_fct",
    comment="Compressed snapshot fact table tracking inventory, cost, and price changes over time.",
    schema="""
        product_fct_key BIGINT GENERATED ALWAYS AS IDENTITY,
        product_id      INT,
        stock_quantity  INT,
        unit_cost       FLOAT,
        unit_price      FLOAT,
        __START_AT      DATE,
        __END_AT        DATE,
        __ACTIVE        BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

dlt.apply_changes(
    target="product_fct",
    source="product_fct_cdc",
    keys=["product_id"],
    sequence_by="source_date",
    stored_as_scd_type=2
)
