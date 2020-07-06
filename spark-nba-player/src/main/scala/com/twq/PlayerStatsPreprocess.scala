package com.twq

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
export HADOOP_CONF_DIR=/Users/xialiu1/apps/hadoop-3.2.1
spark-submit --class com.twq.PlayerStatsPreprocess \
--master yarn \
--executor-memory 512M \
--total-executor-cores 4 \
--executor-cores 2 \
hdfs://localhost:9999/sparkjob/spark-nba-player-1.0-SNAPSHOT.jar \
hdfs://localhost:9999/sparkdata/nba/basketball hdfs://localhost:9999/sparkdata/nba/tmp
 */

object PlayerStatsPreprocess {
  def main(args: Array[String]): Unit = {

    // 兼容yarn和本地模式
    val conf = new SparkConf()
    val configuration = new Configuration()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local")
    }

    val spark = SparkSession.builder()
      .appName("PlayerStatsPreprocess")
      .config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    // 兼容本地和远程传参
    val (rawDataPath, tmpPath) =
      if (args.isEmpty) ("spark-nba-player/data/nba/basketball", "spark-nba-player/data/nba/tmp")
      else {
        // 运行在集群上
        configuration.set("fs.defaultFS", "hdfs://master:9999")
        (args(0), args(1))
      }

    /*
    // 用 hdfs 的接口处理本地文件
    val fs = FileSystem.get(configuration)
    fs.delete(new Path(tmpPath), true)

    // 只处理 1980 之后的
    for (year <- 1980 to 2016) {
      val yearRawData = sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*")
      yearRawData.filter(line => !line.trim.isEmpty && !line.startsWith("Rk,")).map(line => {
        val temp = line.replaceAll("\\*", "").replaceAll(",,", ",0,")
        s"${year},${temp}"   // 把year 合并到行中
      }).saveAsTextFile(s"${tmpPath}/${year}")
    }
     */

    // dataframe 版本
    val rdds: Seq[RDD[(Int, String)]] = (1980 to 2016) map { year =>
      sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*")
        .filter(line => !line.trim.isEmpty && !line.startsWith("Rk,"))
        .map(line => {
            val temp = line.replaceAll("\\*", "").replaceAll(",,", ",0,").replaceAll(",,", ",0,")
            (year, s"${year},${temp}")   // 把year 合并到行中
        })
    }

    // 用 reduce 把rdds合并成一个rdd
    import spark.implicits._
    rdds.reduce(_.union(_)).toDF("year", "line")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year")
        .csv(tmpPath)

    spark.stop()
  }
}
