import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="addresses",
    comment="Deduplicated list of physical addresses for the conformed dimension."
)
def intermediate_addresses():
    customer_address = spark.table("ecomm.staging.customer_address")
    
    return (
        customer_address
        .select(
            col("line_1"),
            col("line_2"),
            col("city"),
            col("state"),
            col("zip")
        )
        .distinct()
    )
