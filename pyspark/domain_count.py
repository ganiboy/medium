from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Domain Visit Aggregation") \
    .getOrCreate()

# Sample input data
data = [
    ("900", "google.mail.com"),
    ("50", "yahoo.com"),
    ("1", "intel.mail.com"),
    ("5", "wiki.org")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["count", "domain"])

# Convert count column to integer
df = df.withColumn("count", df["count"].cast("int"))

# Function to generate subdomains
def generate_subdomains(domain):
    parts = domain.split('.')
    return ['.'.join(parts[i:]) for i in range(len(parts))]

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

generate_subdomains_udf = udf(generate_subdomains, ArrayType(StringType()))
df = df.withColumn("subdomains", generate_subdomains_udf(df["domain"]))

# Explode the subdomains into separate rows
exploded_df = df.withColumn("subdomain", F.explode("subdomains"))

# Group by subdomain and sum the counts
result_df = exploded_df.groupBy("subdomain").agg(F.sum("count").alias("total_count"))

# Order by subdomain
result_df = result_df.orderBy(F.desc("total_count"))
result_df.show()
'''
# Show the result
# result_df.select(F.concat_ws(", ", "total_count", "subdomain")).show(truncate=False)
'''
# Stop the Spark session
spark.stop()