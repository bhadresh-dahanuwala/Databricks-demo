import dlt
from pyspark.sql.functions import col, struct, to_json, current_timestamp, explode
from util import _validate, pad_missing_columns

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
            col("source_date"),
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
            col("source_date"),
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
            col("source_date"),
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
            col("source_date"),
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
            col("source_date"),
            col("customer_id"),
            explode(col("addresses")).alias("address")
        )
        .select(
            col("source_date"),
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
            col("source_date"),
            to_json(struct("*")).alias("raw_record"),
            current_timestamp().alias("quarantined_at"),
        )
    )
