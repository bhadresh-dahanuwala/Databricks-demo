import dlt
from pyspark.sql.functions import *

# You can access bundle variables or Spark configs injected by the pipeline.
# For example, we assume the catalog and schema are passed as variables or we default them.
# Hardcoding a placeholder here, but you can replace with your actual volume path.
VOLUME_PATH = "/Volumes/ecomm/default/raw_vol"

@dlt.table(
    name="raw.customer",
    comment="Raw customer data ingested incrementally from the raw_vol volume using Auto Loader."
)
def raw_customer_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # Infer schema and evolve it if it changes
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/raw_customer")
        .option("cloudFiles.inferColumnTypes", True)
        # Read from any yyyymmdd directory (using a wildcard)
        .load(f"{VOLUME_PATH}/*/customers.json")
    )
