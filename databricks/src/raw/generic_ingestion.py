import dlt
from pyspark.sql.functions import col

# Read the dynamic parameter passed by the DLT pipeline configuration
entity = spark.conf.get("entity_name")
VOLUME_PATH = "/Volumes/ecomm/raw/raw_vol"


@dlt.table(
    name=f"{entity}",
    comment=f"Raw {entity} data ingested incrementally using Auto Loader. "
             f"Schema/types are auto-inferred and evolve over time; the "
             f"staging layer enforces the actual expected contract "
             f"(mandatory columns, expected types) independently of "
             f"whatever gets inferred here."
)
def raw_ingestion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")  # supports pretty-printed, multi-line records
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        # Infer schema and evolve it if it changes
        .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/raw_{entity}")
        .option("cloudFiles.inferColumnTypes", "true")
        # Matches files like customers.json, products.json, etc.
        .option("pathGlobFilter", f"{entity}s.json")
        .load(VOLUME_PATH)
    )