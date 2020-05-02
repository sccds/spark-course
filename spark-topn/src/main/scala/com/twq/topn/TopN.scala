package com.twq.topn

import java.net.URL

import com.twq.local.BoundedPriorityQueue
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {

  def main(args: Array[String]): Unit = {
    val topN = 10
    val n = 1000000
    val path = s"data/topn/${n}"
    val savePath = s"result/topn/${n}"
    val urlPath = s"data/url/${n}"
    val urlSavePath = s"result/url/${n}"
    val url1SavePath = s"result/url1/${n}"
    val url2SavePath = s"result/url2/${n}"

    // 单词的 topN
    runningTopN(path, savePath, topN, line => line.split(" ").map((_, 1L)))

    // URL的topN
    runningTopN(urlPath, urlSavePath, topN, line => Iterator.single(getDomain(line), 1L))

    // mapPartition + PriorityQueue
    runningTopN1(urlPath, url1SavePath, topN, line => Iterator.single((getDomain(line), 1L)))

    // API top
    runningTopN2(urlPath, url2SavePath, topN, line => Iterator.single((getDomain(line), 1L)))

  }

  /**
   * 两次 mapPartition 实现topN
   * @param inputPath
   * @param outputPath
   * @param topN
   * @param parseFunc 数据解析函数
   */
  def runningTopN(inputPath: String, outputPath: String, topN: Int,
                  parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    // 运行 spark 程序
    runningSparkJob(createSparkContext, sc => {
      // 读取数据形成 RDD
      val rdd = sc.textFile(inputPath)

      // 操作RDD，求出每个单词出现的次数
      val wordCountRDD = rdd
        .filter(!_.isEmpty)  // 过滤空的数据
        .flatMap(line => parseFunc(line)) // 按照给定函数进行数据转换，并将结果转换为key/value
        .reduceByKey(_ + _) // 聚合每个word出现的总次数

      import scala.collection.mutable.ListBuffer
      val partitionTopN = (buffer: ListBuffer[(String, Long)], currentWordCount: (String, Long)) => {
        buffer += currentWordCount // 将currentWordCount添加到buffer中
        // 先判断buffer中的数据是否达到N个，如果没有达到，直接将currentWordCount添加到buffer中
        // 如果buffer中的数据个数达到N个，将currentWordCount添加到buffer中，然后，去掉最小的那个数据
        val result = if (buffer.size > topN) {
          // 将数据进行排序，然后删除最小，默认排序是升序
          val sortedBuffer = buffer.sortBy(_._2)
          // 删除最小的，最小的在第一个元素
          sortedBuffer.remove(0)
          // 返回删除元素后的集合
          sortedBuffer
        } else {
          // 此时数据没有达到topN个，不需要进行排序和删除操作
          buffer
        }

        // 返回聚合结果
        result
      }

      // 操作RDD，获取出现次数最多的前 topN 的数据
      val topNResultRDD: RDD[(String, Long)] = wordCountRDD.mapPartitions(iter => {
        // 1. 求当前分区出现次数最多的前N个单词
        iter.foldLeft(ListBuffer[(String, Long)]())(partitionTopN).toIterator

      }).repartition(1)  // 重置为一个分区，再进行一次计算
        .mapPartitions(iter => {
          // 1. 求当前分区出现次数最多的前N个domain
          iter.foldLeft(ListBuffer[(String, Long)]())(partitionTopN).sortBy(_._2).reverse.toIterator
        })

      // 结果保存
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)
      topNResultRDD.saveAsTextFile(outputPath)
    })
  }

  /**
   * 使用一次 mapPartition + PriorityQueue 的方式来实现 topN
   * @param inputPath
   * @param outputPath
   * @param topN
   * @param parseFunc
   */
  def runningTopN1(inputPath: String, outputPath: String, topN: Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    runningSparkJob(createSparkContext, sc => {
      // 读取数据形成RDD
      val rdd = sc.textFile(inputPath)

      // 操作RDD，求出每个单词出现的次数
      val wordCountRDD = rdd
        .filter(!_.isEmpty)  // 过滤空数据
        .flatMap(line => parseFunc(line)) // 按照给定函数进行数据转换操作，并将结果转换为 key/value 键值对
        .reduceByKey(_ + _) // 聚合求每个word出现的总次数

      val ord = new Ordering[(String, Long)] {
        override def compare(x: (String, Long), y: (String, Long)): Int = y._2.compare(x._2)
      }

      val mapRDDs: RDD[ScalaBoundedPriorityQueue[(String, Long)]] = wordCountRDD mapPartitions { items =>
        // priority keeps the largest elements, so let's reverse the ordering
        //val queue = new BoundedPriorityQueue[(String, Long)](topN, ord.reversed())
        //queue.addAll(Utils.takeOrdered(items, topN)(ord))

        val queue = new ScalaBoundedPriorityQueue[(String, Long)](topN)(ord.reverse)
        import scala.collection.JavaConversions._
        queue ++= Utils.takeOrdered(items, topN)(ord)

        Iterator.single(queue)
      }

      val result = if (mapRDDs.partitions.length == 0) {
        Array.empty
      } else {
        mapRDDs.reduce { (queue1, queue2) =>
          queue1 ++= queue2
        }.toArray.sorted(ord)
      }

      // 结果保存
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)
      sc.parallelize(result, 1).saveAsTextFile(outputPath)
    })
  }

  /**
   * 通过 spark RDD 中的 top 函数方式
   * @param inputPath
   * @param outputPath
   * @param topN
   * @param parseFunc
   */
  def runningTopN2(inputPath: String, outputPath: String, topN: Int, parseFunc: String => TraversableOnce[(String, Long)]): Unit = {
    // 运行程序
    runningSparkJob(createSparkContext, sc => {
      // 读取数据形成RDD
      val rdd = sc.textFile(inputPath)

      // 操作RDD，求出每个单词出现的次数
      val wordCountRDD = rdd
        .filter(!_.isEmpty)
        .flatMap(line => parseFunc(line))
        .reduceByKey(_ + _)

      // 操作 RDD， 获取出现次数最多的前 topN 的数据
      val topNResult = wordCountRDD.top(topN)(ord = new Ordering[(String, Long)] {
        override def compare(x: (String, Long), y: (String, Long)): Int = x._2.compare(y._2)
      })

      // 结果保存
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputPath), true)
      sc.parallelize(topNResult, 1).saveAsTextFile(outputPath)
    })
  }



  def getDomain(url: String): String = {
    new URL(url).getHost
  }

  def createSparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark topN")
    SparkContext.getOrCreate(conf)
  }

  /**
   * 运行spark程序
   * @param createSparkContext
   * @param operator
   * @param closeSparkContext
   */
  def runningSparkJob(createSparkContext: => SparkContext, operator: SparkContext => Unit,
                      closeSparkContext: Boolean = false): Unit = {
    // 创建上下文
    val sc = createSparkContext

    // 执行并在执行后关闭上下文
    try operator(sc)
    finally if (closeSparkContext) sc.stop()
  }
}
