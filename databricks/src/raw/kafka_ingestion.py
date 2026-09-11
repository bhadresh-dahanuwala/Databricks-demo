# Databricks notebook source
# Ingest raw orders from Confluent Kafka topic into managed Delta table
from pyspark.sql.functions import col, current_date, current_timestamp

# Read Confluent Kafka credentials from Databricks Secret Scope 'confluent'
bootstrap_servers = dbutils.secrets.get(scope="confluent", key="bootstrap-servers")
api_key = dbutils.secrets.get(scope="confluent", key="api-key")
api_secret = dbutils.secrets.get(scope="confluent", key="api-secret")

jaas_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{api_key}" password="{api_secret}";'
)

topic = "ecomm.orders"
target_table = "ecomm.raw.order__kafka"
checkpoint_location = "/Volumes/ecomm/raw/raw_vol/_checkpoints/kafka_orders"

# Ensure target table exists in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        kafka_key STRING,
        kafka_value STRING,
        topic STRING,
        partition INT,
        offset BIGINT,
        kafka_timestamp TIMESTAMP,
        timestamp_type INT,
        ingested_at TIMESTAMP,
        source_date DATE
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"Starting Kafka ingestion from topic '{topic}' into '{target_table}'...")

df_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Apply 1-hour watermark on Kafka message event timestamp and deduplicate duplicate deliveries
df_stream_deduped = (
    df_stream
    .withWatermark("timestamp", "1 hour")
    .dropDuplicates(["key", "timestamp"])
)

query = (
    df_stream_deduped.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "timestampType AS timestamp_type",
        "current_timestamp() AS ingested_at",
        "current_date() AS source_date"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
print(f"Kafka ingestion finished successfully into '{target_table}'.")
