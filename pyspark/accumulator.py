from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Use Accumulator") \
    .getOrCreate()

# Define accumulators
processed_rows_acc = spark.sparkContext.accumulator(0)
def count_rows(value):
    # Increment the accumulator
    processed_rows_acc.add(1)
    return value

# Register the UDF
count_rows_udf = udf(count_rows, IntegerType())
# Create an example DataFrame
data = [(1,), (2,), (3,), (4,)]
df = spark.createDataFrame(data, ["number"])

# Apply the UDF to the DataFrame
df_with_count = df.withColumn("number", count_rows_udf(col("number")))

# Trigger an action to see the effect of the accumulator
df_with_count.show()

# Print the accumulator's value
print(f"Processed rows: {processed_rows_acc.value}")