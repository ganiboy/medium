from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("b_a").getOrCreate()
country_accum = spark.sparkContext.accumulator(0)
country = ['China', 'Japan']
country_broadcast = spark.sparkContext.broadcast(country)

def broadcast_accum(x):
    if x in country_broadcast.value:
        country_accum.add(1)
        return True
    else:
        country_accum.add(1)
        return False

broadcast_accum_udf = udf(broadcast_accum, StringType())

df = spark.read.format("csv") \
    .option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/input/customers-1000.csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load()

df = df.withColumn("Country_flag", broadcast_accum_udf(col("Country")))
print("&&&&&&&ACCUM*******----> 1", country_accum.value)
df.show()
print("&&&&&&&ACCUM*******---> 2", country_accum.value)
country = df.filter("Country in ('China', 'Japan')")
country.show()

country_count = df.select("Country").groupBy("Country").agg(count("*").alias("country_count"))
country_count = country_count.filter("Country in ('China', 'Japan')")
country_count.show(1000)
print("&&&&&&&ACCUM*******---> 3", country_accum.value)
