import dlt
from pyspark.sql.functions import col, current_timestamp

# Add a processing timestamp so apply_changes has a sequence to order by
@dlt.view
def customer_contact_cdc():
    return spark.readStream.table("ecomm.staging.customer_contact").withColumn("processing_time", current_timestamp())

# Define the target SCD table with an identity column for the integer surrogate key
dlt.create_streaming_table(
    name="customer_contact_dim",
    comment="Final Type 2 Dimension for Customer Contacts",
    schema="""
        customer_contact_key BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id INT,
        contact_number STRING,
        processing_time TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP,
        __ACTIVE BOOLEAN
    """
)

# Use Databricks DLT native SCD Type 2 handling
dlt.apply_changes(
    target="customer_contact_dim",
    source="customer_contact_cdc",
    keys=["customer_id", "contact_number"],
    sequence_by="processing_time",
    stored_as_scd_type=2
)
