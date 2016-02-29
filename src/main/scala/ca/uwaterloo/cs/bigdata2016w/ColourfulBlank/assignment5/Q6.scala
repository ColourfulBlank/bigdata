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

   
object Q6 extends Tokenizer {
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
    val conf = new SparkConf().setAppName("A5Q6")
    val sc = new SparkContext(conf)
    val date = args.date().split('-')
    // val customer = sc.textFile(iter.next)// TPC-H-0.1-TXT/customer.tbl
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")// TPC-H-0.1-TXT/lineitem.tbl
    var counter = lineitem
                          .map(line => line.split('|'))
                          .filter(list => {
                              var retbool = true
                              val cDate = list(10).split('-')
                              for (i <- 0 until date.length){
                                if (! date(i).equals(cDate(i))){
                                  retbool = false
                                }
                              }
                              retbool
                          })
                          .map(list => {
                            val l_quantity = list(4).toInt
                            val l_extededprive = list(5).toFloat
                            val l_discount = list(6).toFloat
                            val l_tax = list(7).toFloat
                            ((list(8), list(9)), List(l_quantity, l_extededprive, l_extededprive * (1 - l_discount), l_extededprive * (1 - l_discount) * (1 + l_tax), l_discount, 1))
                            })
                          .reduceByKey((_ , _).zipped.map(_ + _))
                          .map (list => {
                            val ct = list._2(5)
                            (list._1._1, list._1._2, List(list._2(0), list._2(1), list._2(2), list._2(3), list._2(0)/ct, list._2(1)/ct,list._2(4)/ct, ct))
                            })
                          .collect()
                          .foreach(println)


    
  }
}
