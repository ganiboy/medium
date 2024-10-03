from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("word_count").getOrCreate()


sc = spark.sparkContext
input = sc.textFile("/Users/ganeshmoorthy/Desktop/coding/medium/input/word_count.txt")
split_words = input.flatMap(lambda x: x.split(" "))
map_words = split_words.map(lambda x: (x,1))

word_count = map_words.reduceByKey(lambda x,y: x+y)
word_count.collect()
time.sleep(1000)
