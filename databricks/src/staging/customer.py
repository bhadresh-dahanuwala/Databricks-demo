import dlt
from pyspark.sql.functions import col, expr, get_json_object, to_json, struct, current_timestamp

# --- Declared contract for this entity -------------------------------------
# mandatory=True  -> column must be present; missing OR wrong type -> quarantine
# mandatory=False -> column may be absent (fine); if present, wrong type -> quarantine
# "type" is any Spark SQL type name accepted by try_cast, e.g. "int", "string",
# "double", "boolean", "date", "timestamp".
CUSTOMER_SCHEMA = {
    "id":         {"mandatory": True,  "type": "int"},
    "first_name": {"mandatory": True,  "type": "string"},
    "last_name":  {"mandatory": True,  "type": "string"},
    "email":      {"mandatory": False, "type": "string"},
    # add any remaining expected columns here
}


@dlt.view(name="customer_parsed")
def customer_parsed():
    df = spark.readStream.table("ecomm.raw.customer")

    # FIX: If Auto Loader has never seen an optional column (e.g. 'email'), 
    # it won't exist in the raw table schema at all, causing an UNRESOLVED_COLUMN error.
    # We must pad any missing contract columns with NULLs before validating.
    from pyspark.sql.functions import lit
    for c in CUSTOMER_SCHEMA.keys():
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))

    is_invalid = None
    for c, spec in CUSTOMER_SCHEMA.items():
        raw_col = col(c)
        cast_val = expr(f"try_cast(`{c}` AS {spec['type']})")

        # Auto Loader already flags a value that conflicted with the raw
        # layer's inferred schema by nulling the column and recording the
        # original value under this field name in _rescued_data. Consult
        # that to tell "key genuinely absent" apart from "key present but
        # type-conflicting" -- both look like a plain null column otherwise.
        was_rescued = get_json_object(col("_rescued_data"), f"$.{c}").isNotNull()

        missing = raw_col.isNull() & ~was_rescued
        type_conflict = was_rescued | (raw_col.isNotNull() & cast_val.isNull())

        bad = (missing & spec["mandatory"]) | type_conflict
        is_invalid = bad if is_invalid is None else (is_invalid | bad)

        df = df.withColumn(c, cast_val)

    return (
        df.withColumn("_is_invalid", is_invalid)
        .withColumnRenamed("id", "customer_id")
    )


@dlt.table(
    name="customer",
    comment="Cleaned, typed, and validated customer data in the staging layer."
)
def staging_customer():
    return (
        dlt.read_stream("customer_parsed")
        .filter("_is_invalid = false")
        .drop("_is_invalid", "_rescued_data")
    )


@dlt.table(
    name="customer_quarantine",
    comment="Customer records missing a mandatory column or failing the expected data type. "
             "raw_record is reconstructed from the raw layer's parsed columns (plus any "
             "rescued values) -- it reflects the same data as the source, but formatting "
             "(field order, whitespace) is not guaranteed to match the original file."
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