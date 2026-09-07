import dlt
from functools import reduce
from pyspark.sql.functions import col, transform, exists, struct, to_json, current_timestamp

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

def _validate(value_col, spec):
    """
    Returns (casted_column, is_invalid_column) for one field.

    Recurses into "array" fields that declare an element_schema (array of
    structs) -- each element is validated against that sub-schema, and the
    whole array field is flagged invalid if ANY element fails.

    NOTE: at nested levels this treats every null as "missing" regardless of
    cause. At the top level, Auto Loader's own schema-conflict rescue could in
    principle be cross-checked via _rescued_data (see the flat-schema version
    of this file) to distinguish "key absent" from "raw layer already nulled
    it out due to a type conflict" -- but _rescued_data's path format for
    fields nested inside arrays isn't a documented, stable contract, so that
    distinction is deliberately not attempted below this level.
    """
    if spec.get("type") == "array" and "element_schema" in spec:
        element_schema = spec["element_schema"]

        def _elem_casted(x):
            return struct(*[
                _validate(x[name], sub_spec)[0].alias(name)
                for name, sub_spec in element_schema.items()
            ])

        def _elem_invalid(x):
            return reduce(
                lambda a, b: a | b,
                [_validate(x[name], sub_spec)[1] for name, sub_spec in element_schema.items()],
            )

        casted_array = transform(value_col, _elem_casted)
        any_elem_invalid = exists(value_col, _elem_invalid)

        missing = value_col.isNull()  # treat an empty array as present-but-empty, not missing;
                                       # add "| (size(value_col) == 0)" here if empty should count as missing
        is_invalid = (missing & spec["mandatory"]) | (value_col.isNotNull() & any_elem_invalid)
        return casted_array, is_invalid

    # leaf field: scalar, or an array of primitives (e.g. array<string>)
    cast_val = value_col.cast(spec["type"])
    missing = value_col.isNull()
    is_invalid = (missing & spec["mandatory"]) | (value_col.isNotNull() & cast_val.isNull())
    return cast_val, is_invalid

def pad_missing_columns(df, schema):
    from pyspark.sql.functions import lit
    for c in schema.keys():
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))
    return df

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
    name="order_quarantine",
    comment="Order records missing a mandatory field or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_order_quarantine():
    return (
        dlt.read_stream("order_parsed")
        .filter("_is_invalid = true")
        .select(
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
