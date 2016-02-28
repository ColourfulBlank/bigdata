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

   
object Q3 extends Tokenizer {
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
    val conf = new SparkConf().setAppName("A5Q3")
    val sc = new SparkContext(conf)
    val date = args.date().split('-')
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")// TPC-H-0.1-TXT/lineitem.tbl
    val part = sc.textFile(args.input() + "/part.tbl")
    val supplier = sc.textFile(args.input() + "/supplier.tbl")
    val broadCastPart = sc.broadcast(
                                      part
                                      .map(line => {
                                        val list = line.split('|')
                                        (list(0), list(1))
                                      })
                                      .collectAsMap
                                      )
    val broadCastSupplier = sc.broadcast(
                                          supplier
                                          .map(line => {
                                            val list = line.split('|')
                                            (list(0), list(1))
                                          })
                                          .collectAsMap
                                        )
    val lineSimp = lineitem
                            .map(line => {
                              val list = line.split('|')
                              List(list(0), list(10), list(1), list(2))//l_orderkey, l_shipdate, l_partkey, l_suppkey
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
                            .map(list => (list(0).toInt, (broadCastPart.value.getOrElse(list(2), ""),broadCastSupplier.value.getOrElse (list(3), ""))))
                            .sortByKey()
                            .take(20)
                            .map(list => (list._1, list._2._1, list._2._2))
                            .foreach(println)
  }
}
