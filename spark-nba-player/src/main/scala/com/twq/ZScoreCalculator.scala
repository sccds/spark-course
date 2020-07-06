package com.twq
/*
spark-submit --class com.twq.ZScoreCalculator \
--master yarn \
--conf spark.sql.warehouse.dir=hdfs://localhost:9999/user/hive/warehouse \
--executor-memory 512M \
--total-executor-cores 4 \
--executor-cores 2 \
hdfs://localhost:9999/sparkjob/spark-nba-player-1.0-SNAPSHOT.jar \
hdfs://localhost:9999/sparkdata/nba/tmp
*/

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object ZScoreCalculator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local")
    }

    val spark = SparkSession.builder()
      .appName("ZScoreCalculator")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    // 兼容本地和远程传参
    val yearRawDataPath = if (args.isEmpty) "spark-nba-player/data/nba/tmp" else args(0)

    // 1. 计算每一年每一个指标的平均数和标准方差
    // 1.1 先要解析预处理之后的csv文件，生成PlayStats的Dataset
    import spark.implicits._
    val yearRawStats = sc.textFile(s"$yearRawDataPath/*/*")
    val parsedPlayerStats: Dataset[PlayerStats] = yearRawStats.flatMap(PlayerStats.parse(_)).toDS()
    parsedPlayerStats.printSchema()
    parsedPlayerStats.cache()

    // 1.2 计算每一年的9个指标的平均值和标准方差
    import org.apache.spark.sql.functions._
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    val aggStats: DataFrame = parsedPlayerStats.select($"year", $"rawStats.FGP".as("fgp"), $"rawStats.FTP".as("ftp"),
      $"rawStats.threeP".as("tp"), $"rawStats.TRB".as("trb"), $"rawStats.AST".as("ast"),
      $"rawStats.STL".as("stl"), $"rawStats.BLK".as("blk"), $"rawStats.TOV".as("tov"),
      $"rawStats.PTS".as("pts"))
      .groupBy($"year")
      .agg(
        avg($"fgp").as("fgp_avg"), avg($"ftp").as("ftp_avg"), avg($"tp").as("tp_avg"),
        avg($"trb").as("trb_avg"), avg($"ast").as("ast_avg"), avg($"stl").as("stl_avg"),
        avg($"blk").as("blk_avg"), avg($"tov").as("tov_avg"), avg($"pts").as("pts_avg"),
        stddev($"tp").as("tp_stddev"), stddev($"trb").as("trb_stddev"), stddev($"ast").as("ast_stddev"),
        stddev($"stl").as("stl_stddev"), stddev($"blk").as("blk_stddev"), stddev($"tov").as("tov_stddev"),
        stddev($"pts").as("pts_stddev"))

    // 把上面的数据，组织成 Map 的格式， Map("2016_pts_stddev" -> 2.098, ...) 后面需要查找聚合指标的时候，直接把值拼起来查找就行
    // 因为聚合的数据量小，所以可以到driver端操作
    // 把所有的按照Row的Map放入一个Map里面去
/*    val rows: Array[Row] = aggStats.collect()
    val maps: Seq[Map[String, Double]] = rows.map { row =>
      val year = row.getAs[Int]("year")
      val valueMap = row.getValuesMap[Double](row.schema.map(_.name).filterNot(_.equals("year")))
      valueMap.map { case (key, value) => (s"${year}_${key}", value) }
    }*/

    val aggStatsMap: Map[String, Double] = row2Map(aggStats)

    // 2. 计算每个球员的每年的每个指标的zscore，以及9个指标zscore的总值
    // aggStatsMap 是在driver端进行计算的，在executor计算的时候需要分发，把它广播
    val aggStatsMapB = sc.broadcast(aggStatsMap)
    val statsWithZScore: Dataset[PlayerStats] = parsedPlayerStats.map(PlayerStats.calculateZScore(_, aggStatsMapB.value))
    statsWithZScore.printSchema()

    // 3. 计算标准化的zscore
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    val zStats = statsWithZScore.select($"year", $"zScoreStats.FGP".as("fgw"), $"zScoreStats.FTP".as("ftw"),
      $"ZScoreStats.threeP".as("tp"), $"ZScoreStats.TRB".as("trb"), $"ZScoreStats.AST".as("ast"), $"ZScoreStats.BLK".as("blk"),
      $"ZScoreStats.STL".as("stl"), $"ZScoreStats.TOV".as("tov"), $"ZScoreStats.PTS".as("pts"))
      .groupBy($"year")
      .agg(
        avg($"fgw").as("fgw_avg"), avg($"ftw").as("ftw_avg"),
        stddev($"fgw").as("fgw_stddev"), stddev($"ftw").as("ftw_stddev"),
        min($"fgw").as("fgw_min"), min($"ftw").as("ftw_min"),
        max($"fgw").as("fgw_max"), max($"ftw").as("ftw_max"),
        min($"tp").as("tp_min"), min($"trb").as("trb_min"),
        max($"tp").as("tp_max"), max($"trb").as("trb_max"),
        min($"ast").as("ast_min"), min($"blk").as("blk_min"),
        max($"ast").as("ast_max"), max($"blk").as("blk_max"),
        min($"stl").as("stl_min"), min($"tov").as("tov_min"),
        max($"stl").as("stl_max"), max($"tov").as("tov_max"),
        min($"pts").as("pts_min"), max($"pts").as("pts_max"))

    val zStatsMap: Map[String, Double] = row2Map(zStats)
    val zStatsMapB = sc.broadcast(zStatsMap)

    val statsWithNZScore = statsWithZScore.map(PlayerStats.calculateNZScore(_, zStatsMapB.value))
    statsWithNZScore.printSchema()

    // 4. 计算每一个球员的每年的经验值（等于球员从事篮球比赛的年份）
    // 某球员1980年开始自己的职业生涯 -> 20岁，到了2000年，40岁，经验值为 20
    // 4.1 先将 schema 打平，可以用 schema + RDD[Row]
    val schemaN = StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("GP", IntegerType, true) ::
        StructField("GS", IntegerType, true) ::
        StructField("MP", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    val playerRowRDD = statsWithNZScore.rdd.map { player =>
      Row.fromSeq(Array(player.name, player.year, player.age, player.position, player.team, player.GP, player.GS, player.MP)
          ++ player.rawStats.productIterator.map(_.asInstanceOf[Double])
          ++ player.zScoreStats.get.productIterator.map(_.asInstanceOf[Double])
          ++ Array(player.totalZScores)
          ++ player.nzScoreStats.get.productIterator.map(_.asInstanceOf[Double])
          ++ Array(player.totalNZScore))
    }

    val playerDF = spark.createDataFrame(playerRowRDD, schemaN)

    // 4.2 求每个球员的经验值
    playerDF.createOrReplaceTempView("player")

    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val playerStatsZ = spark.sql("select (p.age - t.min_age) as exp, p.* from player as p " +
      "join (select name, min(age) as min_age from player group by name) as t on p.name = t.name")

    // 5. 结果写到文件中
    //playerStatsZ.write.mode(SaveMode.Overwrite).csv("spark-nba-player/data/nba/playerStatsZ")
    playerStatsZ.write.mode(SaveMode.Overwrite).csv("hdfs://localhost:9999/sparkdata/nba/playerStatsZ")

    //spark.sql("CREATE DATABASE IF NOT EXISTS nba")
    playerStatsZ.write.mode(SaveMode.Overwrite).saveAsTable("nba.player")

    spark.stop()
  }

  private def row2Map(statsDF: DataFrame) = {
    statsDF.collect().map { row =>
      val year = row.getAs[Int]("year")
      val valueMap = row.getValuesMap[Double](row.schema.map(_.name).filterNot(_.equals("year")))
      valueMap.map { case (key, value) => (s"${year}_${key}", value) }
    }.reduce(_ ++ _)
  }

}
