import dlt
from functools import reduce
from pyspark.sql.functions import col, try_cast, transform, exists, struct, to_json, current_timestamp, explode

# --- Declared contract for this entity -------------------------------------
# mandatory=True  -> field must be present; missing OR wrong type -> quarantine
# mandatory=False -> field may be absent (fine); if present, wrong type -> quarantine
# "type" is any Spark SQL type name accepted by try_cast (e.g. "int", "string",
# "array<string>"), OR "array" + "element_schema" for an array of structs,
# where element_schema is itself a dict in this same shape (can nest further).
CUSTOMER_SCHEMA = {
    "id":         {"mandatory": True,  "type": "int"},
    "first_name": {"mandatory": True,  "type": "string"},
    "last_name":  {"mandatory": True,  "type": "string"}
}

CUSTOMER_CONTACT_SCHEMA = {
    "id":         {"mandatory": True,  "type": "int"},
    "first_name": {"mandatory": True,  "type": "string"},
    "last_name":  {"mandatory": True,  "type": "string"},
    "contact_numbers": {"mandatory": True, "type": "array<string>"}
}

CUSTOMER_ADDRESS_SCHEMA = {
    "id":         {"mandatory": True,  "type": "int"},
    "first_name": {"mandatory": True,  "type": "string"},
    "last_name":  {"mandatory": True,  "type": "string"},
    "addresses": {
        "mandatory": True,
        "type": "array",
        "element_schema": {
            "address_type": {"mandatory": True,  "type": "string"},
            "line_1":       {"mandatory": True,  "type": "string"},
            "line_2":       {"mandatory": False, "type": "string"},
            "city":         {"mandatory": True,  "type": "string"},
            "state":        {"mandatory": True,  "type": "string"},
            "zip":          {"mandatory": True,  "type": "string"},
        },
    },
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
    cast_val = try_cast(value_col, spec["type"])
    missing = value_col.isNull()
    is_invalid = (missing & spec["mandatory"]) | (value_col.isNotNull() & cast_val.isNull())
    return cast_val, is_invalid


def pad_missing_columns(df, schema):
    from pyspark.sql.functions import lit
    for c in schema.keys():
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))
    return df

@dlt.view(name="customer_parsed")
def customer_parsed():
    df = spark.readStream.table("ecomm.raw.customer")
    df = pad_missing_columns(df, CUSTOMER_SCHEMA)

    is_invalid = None
    for c, spec in CUSTOMER_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "customer_id")
    )


@dlt.table(
    name="customer",
    comment="Cleaned, typed, and validated customer data (including nested addresses) in the staging layer."
)
def staging_customer():
    return (
        dlt.read_stream("customer_parsed")
        .filter("_is_invalid = false")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name")
        )
    )

@dlt.table(
    name="customer_quarantine",
    comment="Customer records missing a mandatory field (at any nesting level) or failing an expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns -- it reflects the same data as "
             "the source, but formatting (field order, whitespace) is not guaranteed to match the original file."
)
def staging_customer_quarantine():
    return (
        dlt.read_stream("customer_parsed")
        .filter("_is_invalid = true")
        .select(
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )

@dlt.view(name="customer_contact_parsed")
def customer_contact_parsed():
    df = spark.readStream.table("ecomm.raw.customer")
    df = pad_missing_columns(df, CUSTOMER_CONTACT_SCHEMA)

    is_invalid = None
    for c, spec in CUSTOMER_CONTACT_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "customer_id")
    )

@dlt.table(
    name="customer_contact",
    comment="One-to-many relationship table containing customer_id and contact_number."
)
def staging_customer_contact():
    return (
        dlt.read_stream("customer_contact_parsed")
        .filter("_is_invalid = false")
        .select(
            col("customer_id"),
            explode(col("contact_numbers")).alias("contact_number")
        )
    )

@dlt.table(
    name="customer_contact_quarantine",
    comment="Customer contact records missing a mandatory field or failing an expected data type."
)
def staging_customer_contact_quarantine():
    return (
        dlt.read_stream("customer_contact_parsed")
        .filter("_is_invalid = true")
        .select(
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )

@dlt.view(name="customer_address_parsed")
def customer_address_parsed():
    df = spark.readStream.table("ecomm.raw.customer")
    df = pad_missing_columns(df, CUSTOMER_ADDRESS_SCHEMA)

    is_invalid = None
    for c, spec in CUSTOMER_ADDRESS_SCHEMA.items():
        casted, bad = _validate(col(c), spec)
        is_invalid = bad if is_invalid is None else (is_invalid | bad)
        df = df.withColumn(c, casted)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "customer_id")
    )

@dlt.table(
    name="customer_address",
    comment="One-to-many relationship table containing customer_id and flattened address fields."
)
def staging_customer_address():
    return (
        dlt.read_stream("customer_address_parsed")
        .filter("_is_invalid = false")
        .select(
            col("customer_id"),
            explode(col("addresses")).alias("address")
        )
        .select(
            col("customer_id"),
            col("address.address_type"),
            col("address.line_1"),
            col("address.line_2"),
            col("address.city"),
            col("address.state"),
            col("address.zip")
        )
    )

@dlt.table(
    name="customer_address_quarantine",
    comment="Customer address records missing a mandatory field or failing an expected data type."
)
def staging_customer_address_quarantine():
    return (
        dlt.read_stream("customer_address_parsed")
        .filter("_is_invalid = true")
        .select(
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
