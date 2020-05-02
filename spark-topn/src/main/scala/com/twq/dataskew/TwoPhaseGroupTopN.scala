package com.twq.dataskew

import java.util.concurrent.ThreadLocalRandom

import com.twq.topn.GroupTopN.{createSparkContext, runningSparkJob}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey 存在OOM异常
 * 解决方案：采用两阶段聚合操作
 * key前面加一个随机数，使很大的key分散 比如  java,78 => 3-java,78
 */
object TwoPhaseGroupTopN {
  def main(args: Array[String]): Unit = {
    val n = 3
    runningSparkJob(createSparkContext, sc => {
      val lines = sc.textFile("data/grouptopn/test.txt")
      val kvRDD = lines.map(line => {
        val temp = line.split(",")
        (temp(0), temp(1).toInt)
      })
      val random = ThreadLocalRandom.current()
      val randomKeyRDD = kvRDD.map { case (key, value) =>
        // 第一阶段第一步：在key前面加一个随机数
        ((random.nextInt(24), key), value)
      }
      val groupPhaseOne = randomKeyRDD.groupByKey()
      val groupTopNPhaseOne = groupPhaseOne.flatMap { case (key, iter) =>
        // key: (3, java)
        val word = key._2
        // 2,1,3,4,6,5 => 1,2,3,4,5,6 => 取右边三个 4,5,6 => 翻转 6,5,4
        val topN = iter.toList.sorted.takeRight(n).reverse
        // java 6
        // java 5
        // java 4
        topN.map((word, _))
      }

      val groupPhaseTwo = groupTopNPhaseOne.groupByKey()
      val groupTopNPhaseTwo = groupPhaseTwo.map { case (key, iter) =>
        (key, iter.toList.sorted.takeRight(n).reverse)
      }

      FileSystem.get(sc.hadoopConfiguration).delete(new Path("result/grouptopn"), true)
      groupTopNPhaseTwo.repartition(1).saveAsTextFile("result/grouptopn")
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


