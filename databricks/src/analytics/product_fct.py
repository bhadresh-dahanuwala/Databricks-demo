import dlt

@dlt.view
def product_fct_cdc():
    df_product_staging = spark.readStream.table("ecomm.staging.product")
    df_product_dim = spark.table("ecomm.analytics.product_dim")
    
    return df_product_staging.join(
        df_product_dim,
        on="product_id",
        how="inner"
    ).select(
        df_product_staging["product_id"],
        df_product_dim["product_key"],
        df_product_staging["stock_quantity"],
        df_product_staging["unit_cost"],
        df_product_staging["unit_price"],
        df_product_staging["source_date"]
    )

dlt.create_streaming_table(
    name="product_fct",
    comment="Compressed snapshot fact table tracking inventory, cost, and price changes over time.",
    schema="""
        product_key     BIGINT,
        product_id      INT,
        stock_quantity  INT,
        unit_cost       FLOAT,
        unit_price      FLOAT,
        source_date     DATE,
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
