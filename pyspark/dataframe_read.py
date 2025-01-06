from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("dataframe_read").getOrCreate()
df = spark.read.format("csv").option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/input/customers-1000.csv").load()
time.sleep(1000)