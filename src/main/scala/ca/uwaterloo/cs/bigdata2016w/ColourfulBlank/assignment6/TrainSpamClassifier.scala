package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment6.Conf_TrainSpamClassifier


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import scala.math._
import scala.util.Random

import java.util.Date
import java.io.File
import java.text.DateFormat

   
object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  // w is the weight vector (make sure the variable is within scope)
      def tryThis(iter: Iterator[(Int, Iterable[(String, Double, Array[Int])])]) : Iterator[(Int, Double)] = 
        {
        
          iter.flatMap(kvpairs => {
            var w = scala.collection.mutable.Map[Int, Double]()///need to be board casted
            def spamminess(features: Array[Int]) : Double = {
              var score = 0d
              features.foreach(f => if (w.contains(f)) score += w(f))
              score
            }
           val weight = kvpairs._2.map(instance => {
            // This is the main learner:
              val delta = 0.002
              val isSpam = instance._2   // label
              val features = instance._3 // feature vector of the training instance
              // Update the weights as follows:
              val score = spamminess(features)
              val prob = 1.0 / (1 + exp(-score))
              features.foreach(f => {
                if (w.contains(f)) {
                  w(f) += (isSpam - prob) * delta
                } else {
                  w(f) = (isSpam - prob) * delta
                 } 
              })
            })
           w.map(line => (line._1, line._2)).toIterator
          })
          
        }
  def main(argv: Array[String]) {
    val args = new Conf_TrainSpamClassifier(argv)
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle.isSupplied)
    val shuffle = args.shuffle.isSupplied    
    val conf = new SparkConf().setAppName("=O=")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    var w = scala.collection.mutable.Map[Int, Double]()///need to be board casted
    if (shuffle){
      // println("shuffle")
      val trained = textFile.map(line => {
        val randomkey = Random.nextInt
        (randomkey, line)
        })
      .sortByKey()
      .map(line => line._2)
      .map(line =>{
        // Parse input
        val tokens = line.split(" ")
        val docid = tokens(0)
        var isSpam = 0d;
        if (tokens(1) == "spam"){
          isSpam = 1d;
        }
        var features = tokens.toList.slice(2, tokens.size).map(fe => fe.toInt).toArray

        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      .mapPartitions(tryThis)
      
      trained.saveAsTextFile(args.model())

    } else {
      val trained = textFile.map(line =>{
        // Parse input
        val tokens = line.split(" ")
        val docid = tokens(0)
        var isSpam = 0d;
        if (tokens(1) == "spam"){
          isSpam = 1d;
        }
        var features = tokens.toList.slice(2, tokens.size).map(fe => fe.toInt).toArray

        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      .mapPartitions(tryThis)
      
      trained.saveAsTextFile(args.model())
    }
    

      }
}
