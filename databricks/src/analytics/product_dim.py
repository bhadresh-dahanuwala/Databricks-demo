import dlt

@dlt.view
def product_dim_cdc():
    return (
        spark.readStream.table("ecomm.staging.product")
        .selectExpr("id as product_id", "name as product_name", "category as product_category", "source_date")
    )

dlt.create_streaming_table(
    name="product_dim",
    comment="Product dimension (Type 1) containing stable attributes.",
    schema="""
        product_key      BIGINT GENERATED ALWAYS AS IDENTITY,
        product_id       INT,
        product_name     STRING,
        product_category STRING
    """
)

dlt.apply_changes(
    target="product_dim",
    source="product_dim_cdc",
    keys=["product_id"],
    sequence_by="source_date",
    stored_as_scd_type=1
)
