import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp
from util import _validate, pad_missing_columns

RETURN_SCHEMA = {
    "order_id": {"mandatory": True, "type": "int"},
    "return_request_timestamp": {"mandatory": True, "type": "timestamp"},
    "return_reason": {"mandatory": True, "type": "string"},
    "return_status": {"mandatory": True, "type": "string"}
}

@dlt.view(name="returns_parsed")
def returns_parsed():
    df = spark.readStream.table("ecomm.raw.return")
    df = pad_missing_columns(df, RETURN_SCHEMA)

    is_invalid = None
    for c, spec in RETURN_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
    )

@dlt.table(
    name="returns",
    comment="Cleaned, typed, and validated return data in the staging layer."
)
def staging_returns():
    return (
        dlt.read_stream("returns_parsed")
        .filter("_is_invalid = false")
        .select(
            col("source_date"),
            col("order_id"),
            col("return_request_timestamp"),
            col("return_reason"),
            col("return_status")
        )
    )

@dlt.table(
    name="quarantine.returns",
    comment="Return records missing a mandatory field or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_returns_quarantine():
    return (
        dlt.read_stream("returns_parsed")
        .filter("_is_invalid = true")
        .select(
            col("source_date"),
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
