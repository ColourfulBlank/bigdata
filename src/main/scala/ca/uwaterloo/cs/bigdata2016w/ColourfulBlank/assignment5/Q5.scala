package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5

import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment5.ConfFive


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import java.util.Date
import java.io.File
import java.text.DateFormat

   
object Q5 extends Tokenizer {
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
    val args = new ConfFive(argv)

    log.info("Input: " + args.input())
    println("->Input: " + args.input())
    val conf = new SparkConf().setAppName("A5Q4")
    val sc = new SparkContext(conf)
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
    val orderSimp = orders
                          .map(line => {
                            val list = line.split('|')
                            (list(0), list(1))
                            })
                          .filter(tuple => {//o_custkey = c_custkey
                            val nationkey = broadCastCustomer.value.getOrElse(tuple._2, "")
                            broadCastNation.value.contains(nationkey)
                            })

println("UNITED STATES: ")
    val us = orderSimp
                        .cogroup(lineSimp)
                        .map(line => {
                            (line._1, (line._2._1.map(line => line), line._2._2.map(line => line)))
                          })
                        .filter(x => {!x._2._2.isEmpty && !x._2._1.isEmpty})
                        .map(line => {
                          val nationkey = broadCastCustomer.value(line._2._1.head)
                            (broadCastNation.value.getOrElse(nationkey, ""), line._2._2)
                          })
                        .filter(line => {
                          line._1.equals("UNITED STATES") 
                          })
                        .map(line => {
                          val list = line._2
                          val count = list.map(date => {
                                              date.substring(0, 7) -> 1 
                                            })
                          count
                          })
                        .flatMap(list => list)
                        .reduceByKey(_ + _)
                        .sortBy(_._2)
                        .collect()
                        .foreach(println)
println("CANADA: ")
    val ca = orderSimp
                        .cogroup(lineSimp)
                        .map(line => {
                            (line._1, (line._2._1.map(line => line), line._2._2.map(line => line)))
                          })
                        .filter(x => {!x._2._2.isEmpty && !x._2._1.isEmpty})
                        .map(line => {
                          val nationkey = broadCastCustomer.value(line._2._1.head)
                            (broadCastNation.value.getOrElse(nationkey, ""), line._2._2)
                          })
                        .filter(line => {
                          line._1.equals("CANADA")
                          })
                        .map(line => {
                          val list = line._2
                          val count = list.map(date => {
                                              date.substring(0, 7) -> 1 
                                            })
                          count
                          })
                        // .count()
                        // println(ca)
                        .flatMap(list => list) 
                        .reduceByKey(_ + _)
                        .sortBy(_._2)
                        .collect()
                        .foreach(println)
                                              // .foreach(println)
    // println("ASDASDASD")

                            
    // val f = orderSimp
    //                 .cogroup(lineSimp)
    //                 .map(line => {
    //                     (line._1, (line._2._1.map(line => line), line._2._2.map(line => line)))
    //                   })
    //                 .filter(x => {!x._2._2.isEmpty})
    //                 .map(line => {
    //                     (line._1.toInt, line._2._1.head)
    //                   })
    //                 .sortByKey()
    //                 .take(20)
    //                 .foreach(println)
  }
}
