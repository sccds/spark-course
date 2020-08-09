package com.twq.streaming.receiver

import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.Queue

object QueueStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("QueueStream")
    val sc = new SparkContect(sparkConf)

    // create the context
    val ssc = new StreamingContext(sc, Seconds(1))

    // 创建一个RDD类型的queue
    val rddQueue = new Queue[RDD[Int]]()

    // 创建QueueInputDStream 且接收数据和处理数据
    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    // 将RDD push 到queue中，事实处理
    rddQueue += ssc.sparkContext.makeRDD(1 to 100, 10)

    ssc.stop(false)
  }
}
