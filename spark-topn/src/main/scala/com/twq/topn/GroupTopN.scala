package com.twq.topn

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {
  def main(args: Array[String]): Unit = {
    val n = 3
    runningSparkJob(createSparkContext, sc => {
      val lines = sc.textFile("data/grouptopn/test.txt")
      val kvRDD = lines.map(line => {
        val temp = line.split(",")
        (temp(0), temp(1).toInt)
      })
      val group = kvRDD.groupByKey() // 将相同的key以及对应的所有分数放到同一个分区里
      val groupTopN = group.map { case (key, iter) =>
        // 有可能出现 OOM
        // 这个key所在的这个task会非常慢，数据倾斜
        (key, iter.toList.sorted(Ordering.Int.reverse).take(n))
      }
      FileSystem.get(sc.hadoopConfiguration).delete(new Path("result/grouptopn"), true)
      groupTopN.repartition(1).saveAsTextFile("result/grouptopn")
    })
  }

  def createSparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark topN")
    SparkContext.getOrCreate(conf)
  }

  def runningSparkJob(createSparkContext: => SparkContext, operator: SparkContext => Unit,
                      closeSparkContext: Boolean = false): Unit = {
    // 创建上下文
    val sc = createSparkContext

    // 执行并在执行后关闭上下文
    try operator(sc)
    finally if (closeSparkContext) sc.stop()
  }
}
