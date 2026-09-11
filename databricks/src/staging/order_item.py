import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp, current_date, lit, coalesce, from_json, explode_outer
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from util import _validate, pad_missing_columns

KAFKA_ORDER_ITEMS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("order_items", ArrayType(StructType([
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])), True)
])

ORDER_ITEM_SCHEMA = {
    "order_id":   {"mandatory": True, "type": "int"},
    "product_id": {"mandatory": True, "type": "int"},
    "quantity":   {"mandatory": True, "type": "int"},
    "updated_at": {"mandatory": False, "type": "timestamp"}
}

def _parse_order_item(df, source_name):
    if "source_date" not in df.columns:
        df = df.withColumn("source_date", current_date())
    df = pad_missing_columns(df, ORDER_ITEM_SCHEMA)

    is_invalid = None
    for c, spec in ORDER_ITEM_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    df = df.withColumn("updated_at", coalesce(col("updated_at"), col("source_date").cast("timestamp")))

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumn("source_system", lit(source_name))
    )

@dlt.view(name="order_item_parsed__adls")
def order_item_parsed_adls():
    df = spark.readStream.table("ecomm.raw.order_item__adls")
    return _parse_order_item(df, "ADLS")

@dlt.view(name="order_item_parsed__postgres")
def order_item_parsed_postgres():
    df = spark.readStream.option("skipChangeCommits", "true").table("ecomm.raw.order_item__postgres")
    return _parse_order_item(df, "POSTGRES")

@dlt.view(name="order_item_parsed__kafka")
def order_item_parsed_kafka():
    df = spark.readStream.option("skipChangeCommits", "true").table("ecomm.raw.order__kafka")
    parsed_df = (
        df.withColumn("_parsed", from_json(col("kafka_value"), KAFKA_ORDER_ITEMS_SCHEMA))
        .select(
            col("source_date"),
            col("_parsed.id").alias("parent_order_id"),
            col("_parsed.updated_at").alias("order_updated_at"),
            col("_parsed.order_timestamp").alias("order_timestamp"),
            explode_outer(col("_parsed.order_items")).alias("item")
        )
        .select(
            col("source_date"),
            col("parent_order_id").alias("order_id"),
            col("item.product_id").alias("product_id"),
            col("item.quantity").alias("quantity"),
            coalesce(col("item.updated_at"), col("order_updated_at"), col("order_timestamp")).alias("updated_at")
        )
    )
    return _parse_order_item(parsed_df, "KAFKA")



@dlt.view(name="order_item_parsed")
def order_item_parsed():
    adls = dlt.read_stream("order_item_parsed__adls")
    pg = dlt.read_stream("order_item_parsed__postgres")
    kafka = dlt.read_stream("order_item_parsed__kafka")
    return (
        adls
        .unionByName(pg, allowMissingColumns=True)
        .unionByName(kafka, allowMissingColumns=True)
    )


@dlt.view(name="order_item_valid")
def order_item_valid():
    return (
        dlt.read_stream("order_item_parsed")
        .filter("_is_invalid = false")
        .select(
            col("source_date"),
            col("order_id"),
            col("product_id"),
            col("quantity"),
            col("source_system"),
            col("updated_at")
        )
    )

dlt.create_streaming_table(
    name="order_item",
    comment="Cleaned, typed, and validated order item data in the staging layer. Deduplicated across sources by (order_id, product_id) by latest updated_at/source_date.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="order_item",
    source="order_item_valid",
    keys=["order_id", "product_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1
)

@dlt.table(
    name="quarantine.order_item",
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
