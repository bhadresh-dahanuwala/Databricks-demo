import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp
from util import _validate, pad_missing_columns

PRODUCT_SCHEMA = {
    "id":       {"mandatory": True, "type": "int"},
    "name":     {"mandatory": True, "type": "string"},
    "category": {"mandatory": True, "type": "string"},
    "cost":     {"mandatory": True, "type": "float"},
    "price":    {"mandatory": True, "type": "float"},
    "stock":    {"mandatory": False, "type": "int"}
}

@dlt.view(name="product_parsed")
def product_parsed():
    df = spark.readStream.table("ecomm.raw.product")
    df = pad_missing_columns(df, PRODUCT_SCHEMA)

    is_invalid = None
    for c, spec in PRODUCT_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
    )

@dlt.table(
    name="product",
    comment="Cleaned, typed, and validated product data in the staging layer."
)
def staging_product():
    return (
        dlt.read_stream("product_parsed")
        .filter("_is_invalid = false")
        .select(
            col("source_date"),
            col("id").alias("product_id"),
            col("name").alias("product_name"),
            col("category").alias("product_category"),
            col("cost").alias("unit_cost"),
            col("price").alias("unit_price"),
            col("stock").alias("stock_quantity")
        )
    )

@dlt.table(
    name="quarantine.product",
    comment="Product records missing a mandatory field or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_product_quarantine():
    return (
        dlt.read_stream("product_parsed")
        .filter("_is_invalid = true")
        .select(
            col("source_date"),
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
