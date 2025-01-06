from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("rdd_read").getOrCreate()
sc = spark.sparkContext
input = sc.textFile("/Users/ganeshmoorthy/Desktop/coding/medium/input/customers-1000.csv")
time.sleep(1000)