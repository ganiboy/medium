from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ReadWriteTest").getOrCreate()
df = spark.read.format("csv") \
        .option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/union_input") \
        .option("header", "true")\
        .load()

df = df.drop("Index")
df = df.withColumn("Phone", concat(col("Phone 1"), col("Phone 2")))
df = df.drop("Phone 1").drop("Phone 2")

# for append mode
df.coalesce(1).write.mode("append").format("csv") \
    .option("header", "true") \
    .option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/union_input") \
    .save()

# for overwrite mode with Cache()
df.cache()
df.coalesce(1).write.mode("overwrite").format("csv")\
    .option("header", "true")\
    .option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/union_input")\
    .save()
