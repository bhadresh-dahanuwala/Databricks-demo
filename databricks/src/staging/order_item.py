import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp
from util import _validate, pad_missing_columns

ORDER_ITEM_SCHEMA = {
    "order_id":   {"mandatory": True, "type": "int"},
    "product_id": {"mandatory": True, "type": "int"},
    "quantity":   {"mandatory": True, "type": "int"}
}

@dlt.view(name="order_item_parsed")
def order_item_parsed():
    df = spark.readStream.table("ecomm.raw.order_item")
    df = pad_missing_columns(df, ORDER_ITEM_SCHEMA)

    is_invalid = None
    for c, spec in ORDER_ITEM_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
    )

@dlt.table(
    name="order_item",
    comment="Cleaned, typed, and validated order item data in the staging layer."
)
def staging_order_item():
    return (
        dlt.read_stream("order_item_parsed")
        .filter("_is_invalid = false")
        .select(
            col("source_date"),
            col("order_id"),
            col("product_id"),
            col("quantity")
        )
    )

@dlt.table(
    name="order_item_quarantine",
    comment="Order item records missing a mandatory field or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_order_item_quarantine():
    return (
        dlt.read_stream("order_item_parsed")
        .filter("_is_invalid = true")
        .select(
            col("source_date"),
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
