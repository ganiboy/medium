package dataframe_column_access

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object column_access extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.nanme", "column_access")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val foodSchema = StructType(List(
    StructField("chinese", ArrayType(StringType)),
    StructField("Italian", ArrayType(StringType))
  ))

  val favouritesSchema = StructType(List(
    StructField("color", ArrayType(StringType)),
    StructField("food", foodSchema),
  ))

  val schema = StructType(
    List(
      StructField("Username", StringType, nullable = false),
      StructField("Email", StringType),
      StructField("Identifier", IntegerType),
      StructField("Firstname", StringType),
      StructField("Lastname", StringType),
      StructField("Favourites", StringType)
    )
  )

  val inputDf = spark.read.format("csv")
    .option("path", "inputs/user_details.csv")
    .option("header", "true")
    .schema(schema)
    .option("delimiter", ";")
    .load()

  val df = inputDf.withColumn("Favourites", from_json(col("Favourites"), favouritesSchema))
  df.printSchema()
  df.show(false)

  //  Select Firstname, Lastname and Favourite Colour using column String
  df.select("Username", "Identifier", "Favourites.color", "Favourites.food.chinese").show(false)

  import spark.implicits._
  import org.apache.spark.sql.functions._
  df.select(col("Firstname"), 'Lastname, $"Favourites.color", column("Favourites.food.chinese")).show(false)

  //  column expression
  df.select(concat(col("Firstname"), lit(" "), col("Lastname"))).show(false)

  //  combining column string and column object --> below is error scenario
  //  df.select(col("Firstname"), "Lastname").show(false)

  //  checking $ notation for accessing json column
  df.select($"Favourites.color").show(false)

  //  checking ' notation for accessing json column --> below is error scenario
  //  df.select('Favourites.food.chinese).show(false)

  //combine column string with column expression --> below is error scenario
  //  df.select("Firstname", "concat(Firstname, ' ', Lastname)").show(false)

  //  combine column expression with column object
  df.select($"Firstname", 'Lastname, concat(col("Firstname"), lit(" "), col("Lastname"))).show(false)

  //combine column string with column expression
  df.selectExpr("Firstname", "concat(Firstname, ' ', Lastname)").show(false)
}
