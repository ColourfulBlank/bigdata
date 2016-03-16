package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Conf_ApplySpamClassifier


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._

import java.util.Date
import java.io.File
import java.text.DateFormat

   
object ApplySpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  // w is the weight vector (make sure the variable is within scope)
      
  def main(argv: Array[String]) {
    val args = new Conf_ApplySpamClassifier(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    val conf = new SparkConf().setAppName("=O=")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputDir = new Path(args.output())
    val weightMap = sc.textFile(args.model() + "/part-00000")
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val broadCastMap = sc
      .broadcast( 
        weightMap
          .map(line => line.split(","))
          .map(line => (line(0).stripPrefix("(").trim.toInt, line(1).stripSuffix(")").trim.toDouble))
          .collectAsMap
      )



    val trained = textFile.map(line =>{
      // Parse input
      val tokens = line.split(" ")
      val docid = tokens(0)
      var isSpam = tokens(1)
      var features = tokens.toList.slice(2, tokens.size-1)
      (docid, isSpam, features.map(fe => fe.toInt).toArray)
    })
    .map(instance => {
        def spamminess(features: Array[Int]) : Double = {
          var score = 0d
          features.foreach(f => if (broadCastMap.value.contains(f)) score += broadCastMap.value(f))
          score
        }
        // kvpairs._2.map(instance => {
          // This is the main learner:
            val id = instance._1
            val isSpam = instance._2   // label
            val features = instance._3 // feature vector of the training instance
            // Update the weights as follows:
            val score = spamminess(features)
            if (score >= 0) {
              (id, isSpam, score, "spam")
            } else {
              (id, isSpam, score, "ham")
            }
        // })
      })
    trained.saveAsTextFile(args.output())

      }
}
