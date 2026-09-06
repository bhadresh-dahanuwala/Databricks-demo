import dlt
from pyspark.sql.functions import *

# You can access bundle variables or Spark configs injected by the pipeline.
# For example, we assume the catalog and schema are passed as variables or we default them.
# Hardcoding a placeholder here, but you can replace with your actual volume path.
VOLUME_PATH = "/Volumes/ecomm/raw/raw_vol"

@dlt.table(
    name="raw.product",
    comment="Raw product data ingested incrementally from the raw_vol volume using Auto Loader."
)
def raw_product_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        # Infer schema and evolve it if it changes
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/raw_product")
        .option("cloudFiles.inferColumnTypes", "true")
        # Use pathGlobFilter to find products.json in any subfolder
        .option("pathGlobFilter", "products.json")
        .load(VOLUME_PATH)
    )
