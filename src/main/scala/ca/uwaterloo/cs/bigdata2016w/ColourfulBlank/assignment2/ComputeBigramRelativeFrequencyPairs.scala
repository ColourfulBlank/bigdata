package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2.Conf

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
  
object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  var sum = 0.0f;
  def tryThis(iter: Iterator[(String, Float)]) : Iterator[(String, Float)] = 
    {
      iter.map(curr => {
          var line = tokenize(curr._1)
          if (line.length == 1) {
            sum = curr._2
            (curr._1, sum)
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
