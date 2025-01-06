from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.appName("udf").getOrCreate()


def full_name(x, y):
    return x + "-" + y

name = udf(full_name, StringType())

df = spark.read.format("parquet")\
    .option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/input/userdata1.parquet").load()
df = df.withColumn("full_name", name(col("first_name"), col("last_name")))
df = df.select("registration_dttm", "id", "title", "comments","full_name")
df = df.filter("id = 10")

df.show()
# time.sleep(10000)