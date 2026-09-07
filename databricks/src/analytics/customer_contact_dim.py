import dlt
from pyspark.sql.functions import col

@dlt.view
def customer_contact_cdc():
    return spark.readStream.table("ecomm.staging.customer_contact")

# Define the target SCD table with an identity column for the integer surrogate key
dlt.create_streaming_table(
    name="customer_contact_dim",
    comment="Final Type 2 Dimension for Customer Contacts",
    schema="""
        customer_contact_key BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id INT,
        contact_number STRING,
        source_date DATE,
        __START_AT DATE,
        __END_AT DATE,
        __ACTIVE BOOLEAN GENERATED ALWAYS AS (__END_AT IS NULL)
    """
)

# Use Databricks DLT native SCD Type 2 handling
dlt.apply_changes(
    target="customer_contact_dim",
    source="customer_contact_cdc",
    keys=["customer_id", "contact_number"],
    sequence_by="source_date",
    stored_as_scd_type=2
)
