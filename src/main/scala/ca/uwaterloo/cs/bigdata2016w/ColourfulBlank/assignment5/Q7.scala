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

   
object Q7 extends Tokenizer {
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
    val conf = new SparkConf().setAppName("A5Q7")
    val sc = new SparkContext(conf)
    val strDate:String = args.date()
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(strDate)
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")// TPC-H-0.1-TXT/lineitem.tbl
    val customer = sc.textFile(args.input() + "/customer.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val broadCastCustomer = sc.broadcast(
                                      customer
                                      .map(line => {
                                        val list = line.split('|')
                                        (list(0), list(1))
                                      })
                                      .collectAsMap
                                      ) 
    val ordersSimp = orders.map(line => {
                              val list = line.split('|')
                              List(list(0), list(1), list(4), list(7))//o_orderkey o_custkey o_orderdate o_shippriority
                              })
                              .filter(list => {
                                val cDate = format.parse(list(2))
                                cDate.before(date)
                              })
                              .map(list => (list(0), List(broadCastCustomer.value.getOrElse(list(1), ""), list(2), list(3))))//o_orderkey o_custkey(c_name) o_orderdate o_shippriority
    val lineSimp = lineitem
                            .map(line => {
                              val list = line.split('|')
                              (list(0), (list(10), list(5), list(6)))//l_orderkey, l_shipdate, l_extendedprice, l_discount
                              // (list(0), list(10))
                              })
                            .filter(list => {
                                val cDate = format.parse(list._2._1)

                                cDate.after(date)
                              })
                            .map(list => {
                              val l_extendedprice = list._2._2.toFloat
                              val l_discount = list._2._3.toFloat
                               (list._1, l_extendedprice*(1-l_discount))//l_extendedprice*(1-l_discount)
                              })
                            .cogroup(ordersSimp)
                            .map(line => {
                                (line._1.toInt, line._2._1.map(line => line), line._2._2.map(line => line))
                              })
                            .filter(x => {
                              !x._2.isEmpty && !x._3.isEmpty
                            })
                            .map(line => {
                               (List(line._3.head(0), line._1, line._3.head(1),line._3.head(2)) , line._2.sum/*, line._2.toList.length)*/)
                              })
                            .reduceByKey(_ + _)/*(x, y) => {(x._1 + y._1, x._2 + y._2)} )*/
                            // .map(line => (line._1, line._2._1/line._2._2))
                            .sortBy(_._2)
                            .take(10)
                            // .collect
                            .foreach(println)
  }
}
