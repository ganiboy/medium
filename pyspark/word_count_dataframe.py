from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

filename = "/Users/ganeshmoorthy/Desktop/coding/medium/input/word_count.txt"

df = spark.read.text(filename)
df.show()

df = df.withColumn("value", explode(split(col("value"),(" "))))
df = df.withColumn("value", lower(col("value")))
df.show()
df = df.groupBy("value").agg(count("*").alias("word_count"))
df.show()
