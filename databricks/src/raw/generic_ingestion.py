import dlt
from pyspark.sql.functions import col, regexp_extract, to_date

VOLUME_PATH = "/Volumes/ecomm/raw/raw_vol"
# order and order_item are migrated to Supabase CDC (Lakeflow Connect)
ENTITIES = ["customer", "product", "return", "return_item"]

def generate_raw_table(entity_name):
    @dlt.table(
        name=entity_name,
        comment=f"Raw {entity_name} data ingested incrementally using Auto Loader. "
                 f"Schema/types are auto-inferred and evolve over time; the "
                 f"staging layer enforces the actual expected contract "
                 f"(mandatory columns, expected types) independently of "
                 f"whatever gets inferred here."
    )
    def raw_ingestion():
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")  # supports pretty-printed, multi-line records
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # Infer schema and evolve it if it changes
            .option("cloudFiles.schemaLocation", f"{VOLUME_PATH}/_schemas/raw_{entity_name}")
            .option("cloudFiles.inferColumnTypes", "true")
            # Matches files like customers.json, products.json, etc.
            .option("pathGlobFilter", f"{entity_name}s.json")
            .load(VOLUME_PATH)
        )
        
        return df.withColumn(
            "source_date", 
            to_date(regexp_extract(col("_metadata.file_path"), r"/(\d{8})/", 1), "yyyyMMdd")
        )

# Register all entities dynamically
for entity in ENTITIES:
    generate_raw_table(entity)