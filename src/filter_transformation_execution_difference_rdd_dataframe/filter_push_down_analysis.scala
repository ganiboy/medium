package filter_transformation_execution_difference_rdd_dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filter_push_down_analysis extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "customerApp")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val df = spark.read.format("csv")
          .option("path", "/Users/g0m0467/Desktop/personal/medium/customers-100.csv")
          .option("inferSchema", "true")
          .option("header", "true").load()

  val contcat_phone = df.withColumn("Phone", concat(col("Phone 1"), col("Phone 2")))
  val transform = contcat_phone.filter("Country = 'Chile'")
  val drop_phone = transform.drop("Phone 1").drop("Phone 2")
  drop_phone.show(20)
  drop_phone.explain(true)

  scala.io.StdIn.readLine()

}
