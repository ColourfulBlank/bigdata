package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5


import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Tokenizer


import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  // val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}
  