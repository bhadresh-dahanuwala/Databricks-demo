import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp, to_date, lit
from util import _validate, pad_missing_columns

ORDER_SCHEMA = {
    "id":                   {"mandatory": True,  "type": "int"},
    "order_timestamp":      {"mandatory": True,  "type": "timestamp"},
    "customer_id":          {"mandatory": True,  "type": "int"},
    "order_mode":           {"mandatory": True,  "type": "string"},
    "order_status":         {"mandatory": True,  "type": "string"},
    "return_windows_days":  {"mandatory": True,  "type": "int"},
    "discount_percentage":  {"mandatory": False, "type": "float"},
    "shipping_label":       {"mandatory": False, "type": "string"}
}

def _parse_order(df, source_name):
    if "source_date" not in df.columns:
        df = df.withColumn("source_date", to_date(col("order_timestamp")))
    df = pad_missing_columns(df, ORDER_SCHEMA)

    is_invalid = None
    for c, spec in ORDER_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "order_id")
        .withColumn("source_system", lit(source_name))
    )

@dlt.view(name="order_parsed__adls")
def order_parsed_adls():
    df = spark.readStream.table("ecomm.raw.order__adls")
    return _parse_order(df, "ADLS")

@dlt.view(name="order_parsed__postgres")
def order_parsed_postgres():
    df = spark.readStream.option("skipChangeCommits", "true").table("ecomm.raw.order__postgres")
    return _parse_order(df, "POSTGRES")

@dlt.view(name="order_parsed")
def order_parsed():
    adls = dlt.read_stream("order_parsed__adls")
    pg = dlt.read_stream("order_parsed__postgres")
    return adls.unionByName(pg)

@dlt.view(name="order_valid")
def order_valid():
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
            col("shipping_label"),
            col("source_system")
        )
    )

dlt.create_streaming_table(
    name="order",
    comment="Cleaned, typed, and validated order data in the staging layer. Deduplicated across sources by latest order_timestamp.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="order",
    source="order_valid",
    keys=["order_id"],
    sequence_by="order_timestamp",
    stored_as_scd_type=1
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
