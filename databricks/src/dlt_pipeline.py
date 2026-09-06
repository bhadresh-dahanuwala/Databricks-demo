import dlt
from pyspark.sql.functions import *

@dlt.table(
  comment="The raw e-commerce data.",
  table_properties={"quality": "bronze"}
)
def raw_ecomm_data():
    # Placeholder: In a real pipeline, you would read from cloud storage or a volume
    # return spark.readStream.format("cloudFiles")...
    
    # Returning a dummy dataframe for demonstration
    return spark.createDataFrame(
        [("order_1", 100.0), ("order_2", 250.5)],
        ["order_id", "amount"]
    )

@dlt.table(
  comment="Cleaned and validated e-commerce data.",
  table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
def cleaned_ecomm_data():
    return dlt.read("raw_ecomm_data").withColumn("tax", col("amount") * 0.1)
