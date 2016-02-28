package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Conf


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import java.util.Date
import java.io.File
import java.text.DateFormat

   
object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  var sum = 0.0f;
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory ) {
      d.listFiles.filter(file => { file.isFile() && ! file.isHidden() }).toList
    } else {
      List[File]()
    }
  }
  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    println("->Input: " + args.input())
    println("->Date: " + args.date())
    val conf = new SparkConf().setAppName("A5Q2")
    val sc = new SparkContext(conf)
    val date = args.date().split('-')
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")// TPC-H-0.1-TXT/lineitem.tbl
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val lineSimp = lineitem
                            .map(line => {
                              val list = line.split('|')
                              List(list(0), list(10))
                              })
                            .filter(list => {
                                var retbool = true
                                val cDate = list(1).split('-')
                                for (i <- 0 until date.length){
                                  if (! date(i).equals(cDate(i))){
                                    retbool = false
                                  }
                                }
                                retbool
                            })
                            .map(list => (list(0), list(1)))
                            
    val orderSimp = orders
                          .map(line => {
                            val list = line.split('|')
                            (list(0), list(6))
                            })
    val f = orderSimp
                    .cogroup(lineSimp)
                    .map(line => {
                        (line._1.toInt, (line._2._1.map(line => line), line._2._2.map(line => line)))
                      })
                    .filter(x => {!x._2._2.isEmpty})
                    .sortByKey()
                    .map(line => {
                        (line._2._1.head , line._1)
                      })
                    .take(20)
                    .foreach(println)
  }
}
