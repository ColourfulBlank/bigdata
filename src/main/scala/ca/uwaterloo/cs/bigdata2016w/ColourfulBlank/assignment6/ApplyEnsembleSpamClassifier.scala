package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Conf_ApplyEnsembleSpamClassifier


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._

import java.util.Date
import java.io.File
import java.text.DateFormat

   
object ApplyEnsembleSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  def main(argv: Array[String]) {
    val args = new Conf_ApplyEnsembleSpamClassifier(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())
    val conf = new SparkConf().setAppName("=O=")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputDir = new Path(args.output())
    val method = args.method()
    val weightMap_group_x = sc.textFile(args.model() + "/part-00000")
    val weightMap_group_y = sc.textFile(args.model() + "/part-00001")
    val weightMap_britney = sc.textFile(args.model() + "/part-00002")
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val broadCastMap_group_x = sc
      .broadcast( 
        weightMap_group_x
          .map(line => line.split(","))
          .map(line => (line(0).stripPrefix("(").trim.toInt, line(1).stripSuffix(")").trim.toDouble))
          .collectAsMap
      )
    val broadCastMap_group_y = sc
      .broadcast( 
        weightMap_group_y
          .map(line => line.split(","))
          .map(line => (line(0).stripPrefix("(").trim.toInt, line(1).stripSuffix(")").trim.toDouble))
          .collectAsMap
      )  
    val broadCastMap_britney = sc
      .broadcast( 
        weightMap_britney
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
            // This is the main learner:
            def spamminess_group_x(features: Array[Int]) : Double = {
              var score = 0d
              features.foreach(f => if (broadCastMap_group_x.value.contains(f)) score += broadCastMap_group_x.value(f))
              score
            }
            def spamminess_group_y(features: Array[Int]) : Double = {
              var score = 0d
              features.foreach(f => if (broadCastMap_group_y.value.contains(f)) score += broadCastMap_group_y.value(f))
              score
            }
            def spamminess_britney(features: Array[Int]) : Double = {
              var score = 0d
              features.foreach(f => if (broadCastMap_britney.value.contains(f)) score += broadCastMap_britney.value(f))
              score
            }
              val id = instance._1
              val isSpam = instance._2   // label
              val features = instance._3 // feature vector of the training instance
              // Update the weights as follows:
              val score_group_x = spamminess_group_x(features)
              val score_group_y = spamminess_group_y(features)
              val score_britney = spamminess_britney(features)
              if (method.equals("average")){

                val score = ( score_group_x + score_group_y + score_britney ) / 3 
                if (score > 0) {
                  (id, isSpam, score, "spam")
                } else {
                  (id, isSpam, score, "ham")
                }

              } else if (method.equals("vote")){
                var score = 0;
                if (score_group_x > 0) {
                  score = score + 1;
                }else {
                  score = score - 1;
                }
                if (score_group_y > 0) {
                  score = score + 1;
                }else {
                  score = score - 1;
                }
                if (score_britney > 0) {
                  score = score + 1;
                }else {
                  score = score - 1;
                }
                if (score > 0){
                  (id, isSpam, score, "spam")
                } else {
                  (id, isSpam, score, "ham")
                }
              }
            // instance
            })
    trained.saveAsTextFile(args.output())
  }
}
