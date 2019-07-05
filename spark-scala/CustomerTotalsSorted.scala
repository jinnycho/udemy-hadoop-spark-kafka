package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerTotalsSorted {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val customerID = fields(0)
    val productID = fields(1)
    val dollar = fields(2).toFloat
    (customerID, dollar)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustomerTotals")
    val lines = sc.textFile("../customer-orders.csv")
    val parsedLines = lines.map(parseLine) // (customer: $)
    val customerDollar = parsedLines.reduceByKey((x,y) => x+y)
    val dollarCustomerSorted = customerDollar.map(x => (x._2, x._1)).sortByKey()
    val results = dollarCustomerSorted.collect()
    for (result <- results.sorted) {
      val customerID = result._1
      val dollars = result._2
      println(customerID, "  ", dollars)
    }
  }
}
