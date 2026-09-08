import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp
from util import _validate, pad_missing_columns

ORDER_SCHEMA = {
    "id":                   {"mandatory": True,  "type": "int"},
    "order_timestamp":      {"mandatory": True,  "type": "timestamp"},
    "customer_id":          {"mandatory": True,  "type": "int"},
    "order_mode":           {"mandatory": True,  "type": "string"},
    "order_status":         {"mandatory": True,  "type": "string"},
    "return_windows_days":  {"mandatory": True,  "type": "int"},
    "discount_percentage":  {"mandatory": False,  "type": "float"},
    "shipping_label":       {"mandatory": False,  "type": "string"}
}

@dlt.view(name="order_parsed")
def order_parsed():
    df = spark.readStream.table("ecomm.raw.order")
    df = pad_missing_columns(df, ORDER_SCHEMA)

    is_invalid = None
    for c, spec in ORDER_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "order_id")
    )

@dlt.table(
    name="order",
    comment="Cleaned, typed, and validated order data in the staging layer."
)
def staging_order():
    return (
        dlt.read_stream("order_parsed")
        .filter("_is_invalid = false")
        .select(
            col("source_date"),
            col("order_id"),
            col("order_timestamp"),
            col("customer_id"),
            col("order_mode"),
            col("order_status"),
            col("return_windows_days"),
            col("discount_percentage"),
            col("shipping_label")
        )
    )

@dlt.table(
    name="quarantine.order",
    comment="Order records missing a mandatory field or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_order_quarantine():
    return (
        dlt.read_stream("order_parsed")
        .filter("_is_invalid = true")
        .select(
            col("source_date"),
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
