package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6


import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Tokenizer


import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf_ApplySpamClassifier(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model name", required = true)
}
  