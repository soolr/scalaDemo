package soolr.com.scalaDemo

import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Second
import org.apache.spark.streaming.Seconds

object SimpleDtreamingApp {
  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)

    val events = stream.map { record =>
      val event = record.split(",")
      (event(0), event(1), event(2))
    }
    events.foreachRDD { (rdd, time) =>
      val numPurchases = rdd.count()
      
      rdd.map { case (user, product, price) => price.toDouble }.distinct.count()
      
      // 求有多少个不同客户购买过商品
      val uniqueUsers = rdd.map { case (user, product, price) => user }.distinct().count()
      // 求和得出总收入
      val totalRevenue = rdd.map { case (user, product, price) => price.toDouble }.sum()
      // 求最畅销的产品是什么
      val productsByPopularity = rdd
        .map { case (user, product, price) => (product, 1) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)
      val mostPopular = productsByPopularity(0)
      println("Total purchases: " + numPurchases)
      println("Unique users: " + uniqueUsers)
      println("Total revenue: " + totalRevenue)
      println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}