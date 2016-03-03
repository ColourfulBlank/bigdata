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

   
object Q4 extends Tokenizer {
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
    val conf = new SparkConf().setAppName("A5Q4")
    val sc = new SparkContext(conf)
    val date = args.date().split('-')
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")// TPC-H-0.1-TXT/lineitem.tbl
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val customer = sc.textFile(args.input() + "/customer.tbl")
    val nation = sc.textFile(args.input() + "/nation.tbl")
    val broadCastCustomer = sc.broadcast(
                                          customer
                                          .map(line => {
                                            val list = line.split('|')
                                            (list(0), list(3))//c_custkey, c_nationkey
                                          })
                                          .collectAsMap
                                        )
    val broadCastNation = sc.broadcast(
                                        nation
                                        .map(line => {
                                          val list = line.split('|')
                                          (list(0), list(1))//n_nationkey, n_name
                                        })
                                        .collectAsMap
                                      )
    val lineSimp = lineitem
                            .map(line => {
                              val list = line.split('|')
                              (list(0), list(10))//l_orderkey, l_shipdate
                              })
                            .filter(list => {
                                var retbool = true
                                val cDate = list._2.split('-')
                                for (i <- 0 until date.length){
                                  if (! date(i).equals(cDate(i))){
                                    retbool = false
                                  }
                                }
                                retbool
                            })

    val orderSimp = orders
                          .map(line => {
                            val list = line.split('|')
                            (list(0), list(1))//orderkey, custkey
                            })
                          .filter(tuple => {//o_custkey = c_custkey
                            val nationkey = broadCastCustomer.value.getOrElse(tuple._2, "")
                            broadCastNation.value.contains(nationkey)
                            })

    val end = orderSimp
                        .cogroup(lineSimp)
                        .map(line => {
                            (line._1, (line._2._1.map(line => line), line._2._2.map(line => line)))
                          })
                        .filter(x => {!x._2._2.isEmpty && !x._2._1.isEmpty})
                        .map(line => {
                          val nationkey = broadCastCustomer.value.getOrElse(line._2._1.head, "")
                            ((nationkey, broadCastNation.value.getOrElse(nationkey, "")), line._2._2.toList.length)
                          })
                        .reduceByKey(_ + _)
                        .map(line => (line._1._1.toInt, line._1._2, line._2))
                        .sortBy(_._1.toInt)
                        .collect()
                        .foreach(println)
                        // println(end)

  }
}
