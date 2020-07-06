package com.twq.sql.hive.example

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class UserMovieRatingIntegrationTest extends FlatSpec{
  behavior of "UserMovieRatingIntegrationTest"

  it should "UserMovieRatingIntegrationTest succ" in {
    val spark = SparkSession.builder()
      .appName("UserMovieRatingIntegrationTest")
      .enableHiveSupport()
      .master("local")
      .config("spark.driver.host", "localhost")  // 无网络的时候不加会报错
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.catalog.setCurrentDatabase("twq")
    spark.sql("drop table if exists u_data")
    spark.sql("drop table if exists u_user")
    spark.sql("drop table if exists u_item")

    UserMovieRatingETL.doETL(spark)

    ALSExample.doALS(spark)

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "")
    properties.put("driver", "com.mysql.jdbc.Driver")

    val userRecs = spark.read.jdbc("jdbc:mysql://localhost:3306/user_recs", "userRecs", properties).collect()
    assert(userRecs.nonEmpty)

    spark.stop()
  }
}
