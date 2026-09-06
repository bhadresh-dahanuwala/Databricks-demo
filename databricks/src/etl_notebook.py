# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce ETL Job
# MAGIC This is a basic ETL notebook deployed via Databricks Asset Bundles (DAB).

# COMMAND ----------

print("Starting ETL Process...")

# COMMAND ----------

# Example: Read from external location or raw volume
# df = spark.read.format("csv").load("/Volumes/ecomm/raw/raw_vol/data.csv")
# display(df)

print("ETL Process Completed Successfully!")
