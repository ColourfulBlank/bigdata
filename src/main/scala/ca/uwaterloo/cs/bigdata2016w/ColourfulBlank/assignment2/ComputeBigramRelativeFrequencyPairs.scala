package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}
  
object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  var sum = 0.0f;
  def tryThis(iter: Iterator[(String, Float)]) : Iterator[(String, Float)] = 
    {
      iter.map(curr => {
          if (curr._1.lastIndexOf("*") == curr._1.length() - 1) {
            sum = curr._2
            (curr._1, curr._2)
          } else {
            (curr._1, curr._2/sum)
          }
        })
    }
  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("RF_pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input()) //list of string
    val counts = textFile.flatMap(line => {
          val tokens = tokenize(line)
          if (tokens.length > 1) {
            tokens.sliding(2).flatMap(p => { 
              val pairStar = List(p.head, "*").mkString(" ")
              List(pairStar,p.mkString(" ")) })

          } else { 
            List()
          }
        })
        .map(bigram => (bigram, 1.0f))
        .reduceByKey(_ + _)
        .sortByKey()
        .mapPartitions(tryThis)
        .sortByKey()


      counts.saveAsTextFile(args.output())
  }
}
