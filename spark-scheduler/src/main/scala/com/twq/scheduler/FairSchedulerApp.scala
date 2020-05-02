package com.twq.scheduler

import java.util.concurrent.CyclicBarrier

import org.apache.spark.{SparkConf, SparkContext}

object FairSchedulerApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FairSchedulerApp")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.scheduler.allocation.file", "/Users/xialiu1/Documents/spark-course/spark-scheduler/src/main/resources/fairscheduler.xml")

    val sc = new SparkContext(conf)

    var friendCount = 0L
    var orderCount = 0L

    val barrier = new CyclicBarrier(2, new Runnable {
      override def run(): Unit = {
        println("start save ====")
        sc.setLocalProperty("spark.scheduler.pool", null)
        val total = friendCount + orderCount
        val rdd = sc.parallelize(0 to total.toInt)
        rdd.saveAsTextFile("file:///Users/xialiu1/Documents/spark-course/spark-scheduler/result/FairSchedulerApp")
      }
    })

    new Thread() {
      override def run(): Unit = {
        println("count friend ====")
        // threadlocal 级别
        sc.setLocalProperty("spark.scheduler.pool", "Pool1")
        friendCount = sc.textFile("file:///Users/xialiu1/Documents/spark-course/spark-scheduler/data/friend.txt").count()
        barrier.await()
      }
    }.start()

    new Thread() {
      override def run(): Unit = {
        println("count order ====")
        sc.setLocalProperty("spark.scheduler.pool", "Pool2")
        orderCount = sc.textFile("file:///Users/xialiu1/Documents/spark-course/spark-scheduler/data/order.txt").count()
        barrier.await()
      }
    }.start()

  }
}
