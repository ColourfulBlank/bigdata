package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5


import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Tokenizer


import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfFive(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
}
  