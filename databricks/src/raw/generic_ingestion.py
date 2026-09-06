import dlt
from pyspark.sql.functions import *

# Read the dynamic parameter passed by the DLT pipeline configuration
entity = spark.conf.get("entity_name")
VOLUME_PATH = "/Volumes/ecomm/raw/raw_vol"

@dlt.table(
    name=f"raw.{entity}",
    comment=f"Raw {entity} data ingested incrementally using Auto Loader."
)
def raw_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        # Infer schema and evolve it if it changes
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/raw_{entity}")
        .option("cloudFiles.inferColumnTypes", "true")
        # Matches files like customers.json, products.json, etc.
        .option("pathGlobFilter", f"{entity}s.json")
        .load(VOLUME_PATH)
    )
