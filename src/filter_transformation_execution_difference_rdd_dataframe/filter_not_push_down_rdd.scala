package filter_transformation_execution_difference_rdd_dataframe

import org.apache.spark.SparkContext

object filter_not_push_down_rdd extends App {

  val sc = new SparkContext("local[*]", "filter_not_push_down_rdd")
  val input = sc.textFile("customers-100.csv")
  val flat_map = input.map(_.split(","))
  val select_fields = flat_map.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(9),x(10),x(11), x(7) + " "+x(8)))
  val filter_val = select_fields.filter(x => (x._7 == "Chile"))
  filter_val.collect().foreach(println)
  scala.io.StdIn.readLine()
}
