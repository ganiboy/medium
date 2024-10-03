from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder.appName("agg").getOrCreate()
male_acc = spark.sparkContext.accumulator(0)
female_acc = spark.sparkContext.accumulator(0)
male_sal = spark.sparkContext.accumulator(0)
female_sal = spark.sparkContext.accumulator(0)
test_acc = spark.sparkContext.accumulator(0)
country = ["China", "France"]
broadcast_var = spark.sparkContext.broadcast(country)

def add_accum(salary):
    test_acc.add(1)
    return salary

def full_name(fist_name, last_name, gender, salary):
    test_acc.add(1)
    if gender == "Female":
        female_acc.add(1)
        female_sal.add(salary)
    elif gender == "Male":
        male_acc.add(1)
        male_sal.add(salary)
    else:
        pass
    return fist_name + " - " + last_name

def country(country):
    return True if country in broadcast_var.value else False


# udf
name = udf(full_name, StringType())
test_acc_sal = udf(add_accum, DecimalType())
country_flag = udf(country, BooleanType())


df = spark.read.format("parquet").option("path", "/Users/ganeshmoorthy/Desktop/coding/medium/input/userdata1.parquet").load()
# df.printSchema()
# df.orderBy("salary").show()
# flag = df.select("country").withColumn("flag", country_flag(col("country")))
# flag.show()

full_name = df.withColumn("full_name", name(col("first_name"), col("last_name"), col("gender"), col("salary")))
full_name.show()
# time.sleep(10)
print(male_acc.value, female_acc, male_sal, female_sal, test_acc.value)


'''
gender_count_and_sal = df.groupBy("gender").agg(format_number(sum("salary"),2).alias("gender_salary"))
higher_sal = df.filter("salary > 200000")
# higher_sal.show(10, False)
max_sal = df.agg(max("salary").alias("high_sal"))
# max_sal.show()
df.createOrReplaceTempView("users")
sql_df = spark.sql("select * from users limit 2")
# sql_df.show()



full_name = df.withColumn("full_name", name(col("first_name"), col("last_name"), col("gender"), col("salary")))
full_name.count()
acc = df.withColumn("salary", test_acc_sal(col("salary")))
# acc.count()
# print(male_acc.value, female_acc, male_sal, female_sal, test_acc.value)
'''

