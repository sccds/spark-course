package com.twq.sql.hive.example

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class UserMovieRatingETLTest extends FlatSpec {

  behavior of "UserMovieRatingETLTest"

  it should "doETL succ" in {
    val spark = SparkSession.builder()
      .appName("UserMovieRatingETLTest")
      .enableHiveSupport()
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if (!spark.catalog.databaseExists("twq")) {
      spark.sql("create database twq")
    }
    spark.catalog.setCurrentDatabase("twq")
    spark.sql("drop table if exists u_data")
    spark.sql("drop table if exists u_user")
    spark.sql("drop table if exists u_item")

    UserMovieRatingETL.doETL(spark)

    checkRatingData(spark)
    checkUser(spark)
    checkItem(spark)

    spark.stop()
  }

  private def checkRatingData(spark: SparkSession) = {
    val ratingDataDF = spark.read.table("twq.u_data")
    val ratingData = ratingDataDF.collect()
    assert(ratingData.size == 100000)

    val data1960pt = ratingData.find(row => {
      row.getAs[Int]("user_id") == 196
    })
    assert(data1960pt.get.getAs[Int]("item_id") == 242)
    assert(data1960pt.get.getAs[Int]("rating") == 3)
    assert("1997-12-04 23:55:49".equals(data1960pt.get.getAs[String]("data_time")))
    assert(data1960pt.get.getAs[Int]("year") == 1997)
    assert(data1960pt.get.getAs[Int]("month") == 199712)
  }

  private def checkUser(spark: SparkSession) = {
    val userDF = spark.read.table("twq.u_user")
    val users = userDF.collect()
    assert(users.size == 943)
    val user10pt = users.find(row => {
      row.getAs[Int]("user_id") == 1
    })
    assert(user10pt.get.getAs[Int]("age") == 24)
    assert(user10pt.get.getAs[String]("gender") == "M")
    assert(user10pt.get.getAs[String]("occupation") == "technician")
    assert(user10pt.get.getAs[String]("zip_code") == "85711")
  }

  private def checkItem(spark: SparkSession) = {
    val itemDF = spark.read.table("twq.u_item")
    val items = itemDF.collect()
    assert(items.size == 1682)
    val item10pt = items.find(row => {
      row.getAs[Int]("movie_id") == 1
    })
    assert(item10pt.get.getAs[String]("movie_title") == "Toy Story (1995)")
    assert(item10pt.get.getAs[String]("release_date") == "01-Jan-1995")
    assert(item10pt.get.getAs[String]("video_release_date") == null)
    assert(item10pt.get.getAs[String]("imdb_url") == "http://us.imdb.com/M/title-exact?Toy Story (1995)")
    assert(item10pt.get.getAs[Int]("unknown") == 0)
    assert(item10pt.get.getAs[Int]("action") == 0)
    assert(item10pt.get.getAs[Int]("adventure") == 0)
    assert(item10pt.get.getAs[Int]("animation") == 1)
    assert(item10pt.get.getAs[Int]("children") == 1)
    assert(item10pt.get.getAs[Int]("comedy") == 1)
    assert(item10pt.get.getAs[Int]("crime") == 0)
    assert(item10pt.get.getAs[Int]("documentary") == 0)
    assert(item10pt.get.getAs[Int]("drama") == 0)
    assert(item10pt.get.getAs[Int]("fantasy") == 0)
    assert(item10pt.get.getAs[Int]("film_noir") == 0)
    assert(item10pt.get.getAs[Int]("horror") == 0)
    assert(item10pt.get.getAs[Int]("musical") == 0)
    assert(item10pt.get.getAs[Int]("romance") == 0)
    assert(item10pt.get.getAs[Int]("sci_fi") == 0)
    assert(item10pt.get.getAs[Int]("thriller") == 0)
    assert(item10pt.get.getAs[Int]("war") == 0)
    assert(item10pt.get.getAs[Int]("western") == 0)
  }
}
