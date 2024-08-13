package catalyst_optimizer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit, sum, when}

import scala.language.postfixOps

object catalyst_optimizer extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "catalyst_optimizer")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val df = spark.read.format("parquet").option("path", "input/userdata1.parquet").load()


//  Query Rewriting
  val null_query_rewrite = df.filter(col("salary") === 90263.05)
  null_query_rewrite.show(false)
  null_query_rewrite.explain(true)

  val null_propagation_rules = df.withColumn("new_salary", col("salary")+10 )
  null_propagation_rules.select("salary", "new_salary").show(false)
  null_propagation_rules.explain(true)

//  Predicate Pushdown
  val concat_names = df.withColumn("full_name", concat(col("first_name"),col("last_name")))
  val predicate_push_down_df = concat_names.filter(col("salary") === 90263.05)
  predicate_push_down_df.explain(true)

//  constant_folding
  val constant_folding = df.select(col("id"), lit(50) + lit(70) as "total")
  constant_folding.explain(true)



}
