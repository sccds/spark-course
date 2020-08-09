package com.twq

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

case class AgeOrExpStats(name: String, year: Int, age: Int, exp: Int, zTOT: Double, nTOT: Double)

case class DeltaAgeOrExpStats(ageOrExp: Int, previousZ: Double, previousN: Double, deltaZ: Double, deltaN: Double)

/**
 * 计算球员的价值随着年龄或者经历的变化的趋势
 */
object AgeAndExpTrendAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local")
    }
    val spark = SparkSession.builder()
      .appName("AgeAndExpTrendAnalysis")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val db = if (args.isEmpty) "default" else args(0)

    val playerDF = spark.read.table(s"$db.player")

    import spark.implicits._
    // 1. 拿到需要计算的字段，包括名称，年龄，经历，zTOT, nTOT
    val ageOrExpStatsDS: Dataset[AgeOrExpStats] = playerDF
      .select($"name", $"age", $"experience".as("exp"), $"zTOT", $"nTOT")
      .sort($"name", $"exp".asc)
      .map(row => AgeOrExpStats(row.getAs[String]("name"), row.getAs[Int]("year"),
        row.getAs[Int]("age"), row.getAs[Int]("exp"),
        row.getAs[Double]("zTOT"), row.getAs[Double]("nTOT")))

    val nameGroupedDS: KeyValueGroupedDataset[String, AgeOrExpStats] = ageOrExpStatsDS.groupByKey(_.name)

    // 2. 计算每个人随着年龄的增长，他的价值的变化趋势
    val deltaAgeOrExpStatsDS: Dataset[(ListBuffer[DeltaAgeOrExpStats], ListBuffer[DeltaAgeOrExpStats])] =
      nameGroupedDS.mapGroups { case (_, statsIterator) =>
      // name1 19 0 2 1
      // name1 20 1 3 1.5
      // 每个人不同年龄的各项分值
      var previousZ = 0.0  // 上一个年龄的Zscore
      var previousN = 0.0  // 上一个年龄的Zscore
      val ageBuffer = new ListBuffer[DeltaAgeOrExpStats]()
      val expBuffer = new ListBuffer[DeltaAgeOrExpStats]()
      statsIterator.zipWithIndex.foreach { case (stats, index) => {
        val (deltaZ, deltaN) = if (index == 0) {
          (Double.NaN, Double.NaN)
        } else {
          (stats.zTOT - previousZ, stats.nTOT - previousN)
        }
        previousZ = stats.zTOT
        previousN = stats.nTOT
        ageBuffer += DeltaAgeOrExpStats(stats.age, previousZ, previousN, deltaZ, deltaN)
        expBuffer += DeltaAgeOrExpStats(stats.exp, previousZ, previousN, deltaZ, deltaN)
      }}

      (ageBuffer, expBuffer)
    }

    //3、从特殊到一般，我们需要对所有球员的随着年龄价值的变化进行聚合
    spark.conf.set("spark.sql.shuffle.partitions", 3)
    val ageTrend = deltaAgeOrExpStatsDS.flatMap(_._1).groupBy($"ageOrExp")
      .agg(
        count($"previousZ").as("valueZ_count"), mean($"previousZ").as("valueZ_mean"),
        stddev($"previousZ").as("valueZ_stddev"), max($"previousZ").as("valueZ_max"), min($"previousZ").as("valueZ_min"),
        count($"previousN").as("valueN_count"), mean($"previousN").as("valueN_mean"),
        stddev($"previousN").as("valueN_stddev"), max($"previousN").as("valueN_max"), min($"previousN").as("valueN_min"),
        count($"deltaZ").as("deltaZ_count"), mean($"deltaZ").as("deltaZ_mean"),
        stddev($"deltaZ").as("deltaZ_stddev"), max($"deltaZ").as("deltaZ_max"), min($"deltaZ").as("deltaZ_min"),
        count($"deltaN").as("deltaN_count"), mean($"deltaN").as("deltaN_mean"),
        stddev($"deltaN").as("deltaN_stddev"), max($"deltaN").as("deltaN_max"), min($"deltaN").as("deltaN_min")
      )

    ageTrend.write.mode(SaveMode.Overwrite).saveAsTable(s"${db}.age_trend")

    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val expTrend = deltaAgeOrExpStatsDS.flatMap(_._2).groupBy($"ageOrExp")
      .agg(
        count($"previousZ").as("valueZ_count"), mean($"previousZ").as("valueZ_mean"),
        stddev($"previousZ").as("valueZ_stddev"), max($"previousZ").as("valueZ_max"), min($"previousZ").as("valueZ_min"),
        count($"previousN").as("valueN_count"), mean($"previousN").as("valueN_mean"),
        stddev($"previousN").as("valueN_stddev"), max($"previousN").as("valueN_max"), min($"previousN").as("valueN_min"),
        count($"deltaZ").as("deltaZ_count"), mean($"deltaZ").as("deltaZ_mean"),
        stddev($"deltaZ").as("deltaZ_stddev"), max($"deltaZ").as("deltaZ_max"), min($"deltaZ").as("deltaZ_min"),
        count($"deltaN").as("deltaN_count"), mean($"deltaN").as("deltaN_mean"),
        stddev($"deltaN").as("deltaN_stddev"), max($"deltaN").as("deltaN_max"), min($"deltaN").as("deltaN_min")
      )

    expTrend.write.mode(SaveMode.Overwrite).saveAsTable(s"${db}.exp_trend")

    spark.stop()
  }
}
