import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp, to_date, lit, coalesce, from_json
from pyspark.sql.types import StructType, StructField, StringType
from util import _validate, pad_missing_columns

KAFKA_ORDER_JSON_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_mode", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("return_windows_days", StringType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("shipping_label", StringType(), True),
    StructField("updated_at", StringType(), True)
])

ORDER_SCHEMA = {
    "id":                   {"mandatory": True,  "type": "int"},
    "order_timestamp":      {"mandatory": True,  "type": "timestamp"},
    "customer_id":          {"mandatory": True,  "type": "int"},
    "order_mode":           {"mandatory": True,  "type": "string"},
    "order_status":         {"mandatory": True,  "type": "string"},
    "return_windows_days":  {"mandatory": True,  "type": "int"},
    "discount_percentage":  {"mandatory": False, "type": "float"},
    "shipping_label":       {"mandatory": False, "type": "string"},
    "updated_at":           {"mandatory": False, "type": "timestamp"}
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

    df = df.withColumn("updated_at", coalesce(col("updated_at"), col("order_timestamp")))

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

@dlt.view(name="order_parsed__kafka")
def order_parsed_kafka():
    df = spark.readStream.option("skipChangeCommits", "true").table("ecomm.raw.order__kafka")
    parsed_df = (
        df.withColumn("_parsed", from_json(col("kafka_value"), KAFKA_ORDER_JSON_SCHEMA))
        .select(
            col("source_date"),
            col("_parsed.id").alias("id"),
            col("_parsed.order_timestamp").alias("order_timestamp"),
            col("_parsed.customer_id").alias("customer_id"),
            col("_parsed.order_mode").alias("order_mode"),
            col("_parsed.order_status").alias("order_status"),
            col("_parsed.return_windows_days").alias("return_windows_days"),
            col("_parsed.discount_percentage").alias("discount_percentage"),
            col("_parsed.shipping_label").alias("shipping_label"),
            col("_parsed.updated_at").alias("updated_at")
        )
    )
    return _parse_order(parsed_df, "KAFKA")



@dlt.view(name="order_parsed")
def order_parsed():
    adls = dlt.read_stream("order_parsed__adls")
    pg = dlt.read_stream("order_parsed__postgres")
    kafka = dlt.read_stream("order_parsed__kafka")
    return (
        adls
        .unionByName(pg, allowMissingColumns=True)
        .unionByName(kafka, allowMissingColumns=True)
    )


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
            col("source_system"),
            col("updated_at")
        )
    )

dlt.create_streaming_table(
    name="order",
    comment="Cleaned, typed, and validated order data in the staging layer. Deduplicated across sources by latest updated_at/order_timestamp.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="order",
    source="order_valid",
    keys=["order_id"],
    sequence_by="updated_at",
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
