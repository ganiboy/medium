from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

def broadcast_data(filename):
    with open(filename, 'r') as fh:
        lines = [line.strip() for line in fh]
        print(lines)
    return list(set(lines))

def filter_val(x):
    return x not in broadcast_variable.value


def check_lines(x):
    if x == "":
        myaccum.add(1)


sc = spark.sparkContext
rdd = sc.textFile("/Users/ganeshmoorthy/Desktop/coding/medium/input/word_count.txt")
broadcast_variable = sc.broadcast(broadcast_data("/Users/ganeshmoorthy/Desktop/coding/medium/input/broadcast_file.txt"))
print(broadcast_variable)
myaccum = sc.accumulator(0)
splits = rdd.flatMap(lambda x: x.split(" "))
splits.foreach(check_lines)
# filter_words = splits.filter(lambda x: x not in broadcast_variable.value)
filter_words = splits.filter(filter_val)
map_counter = filter_words.map(lambda x: (x,1))
word_count = map_counter.reduceByKey(lambda x,y: x+y)
for i in word_count.collect():
    print(i)

print(myaccum)


# time.sleep(100000)


# broadcast_data("/Users/ganeshmoorthy/Desktop/coding/medium/input/broadcast_file.txt")