package com.twq.sql.hive.example

import java.util.Properties

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{SaveMode, SparkSession}

object ALSExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    doALS(spark)

    spark.stop()
  }

  def doALS(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1. 准备DataFrame
    val ratings = spark.read.table("twq.u_data")
      .select($"user_id", $"item_id", $"rating".cast("float").as("rating"))

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // 2. ALS算法和训练数据集，产生推荐模型
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user_id")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val model = als.fit(training)

    // 3. 模型评估，计算RMSE，均方根误差
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean_square error = $rmse")

    // 为每个用户推荐5个电影
    val userRecs = model.recommendForAllUsers(5)
      .select($"user_id", explode($"recommendations").as("rec"))
      .select($"user_id", $"rec".getField("item_id").as("item_id"))

    val items = spark.read.table("twq.u_item").select("movie_id", "movie_title")

    // 将结果jdbc保存到 mysql 中
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "")
    properties.put("driver", "com.mysql.jdbc.Driver")

    // 如果在本地测试，需要设置依赖mysql的jdbc驱动
    userRecs.join(items, userRecs.col("item_id") === items.col("movie_id"))
      .select("user_id", "item_id", "movie_title")
      .coalesce(2)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://127.0.0.1:3306/user_recs", "userRecs", properties)

  }
}
