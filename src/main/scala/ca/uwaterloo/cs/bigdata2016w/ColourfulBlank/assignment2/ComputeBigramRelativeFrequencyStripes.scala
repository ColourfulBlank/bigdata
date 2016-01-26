package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment2.Conf

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
  
object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  // def tryThis(iter: Iterator[(String, Map[String, Float])]) : Iterator[(String, Map[String, Float])] = 
  //   {
  //     var sum = 0.0f;
  //     iter.map(curr => {
  //       sum = (curr._2)("*")
  //       (curr._1, curr._2.map{ case(k,v) => {
  //                         if (k != "*"){
  //                             k -> v/sum
  //                           } else {
  //                            k -> sum
  //                           }
  //                         }
  //                       })
  //       })
  //   }


  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("RF_Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input()) //list of string
    var sum = 0.0f;
    val counts = textFile.flatMap(line => {
          val tokens = tokenize(line)
          if (tokens.length > 1) {
            tokens.sliding(2).flatMap(p => { 
              List ((p.head, Map[String, Float](("*", 1.0f), (p.tail.head, 1.0f))))
              })
          } else { 
            List()
          }
        })
        .reduceByKey((map1, map2) => 
           map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0.0f)) }
           )
        // .mapPartitions(tryThis)
        .map(curr => (curr._1, {
          sum = (curr._2)("*")
           curr._2.map{ case(k,v) => {
                            if (k != "*"){
                                k -> v/sum
                              } else {
                               k -> sum
                              }
                            }
                          }
        }))
        .sortByKey()


      counts.saveAsTextFile(args.output())
  }
}
