package com.twq

import java.util.concurrent.TimeUnit

import com.esri.core.geometry.Point
import com.twq.parser.{TaxiTripParser, Trip}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spray.json._
import com.twq.GeoJsonProtocol._

object RunGeoTimeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. 从csv文件中读取出租车载客的记录数据
    val taxiRaw: DataFrame = spark.read.option("header", "true")
      .csv("/Users/xialiu1/Documents/spark-course/spark-geotime/src/main/resources/trip_data.csv")
    taxiRaw.count()
    taxiRaw.show()

    /*
      使用的字段:
      1. hack_license 司机的唯一标识
      2. pickup_datetime  上车时间
      3. dropoff_datetime 下车时间
      4 5. pickup_latitude, pickup_longitude 上车时候经纬度
      6 7. dropoff_latitude, dropoff_longitude 下车时候经纬度
     */

    // 2. 解析出租车载客记录数据
    // 使用 RDD[Row] 对 Row里面的内容进行解析
    //val taxiParsed: RDD[Trip] = taxiRaw.rdd.map(TaxiTripParser.parse)
    val taxiParsed: RDD[Either[Trip, (Row, Exception)]] = taxiRaw.rdd.map(safe(TaxiTripParser.parse))

    // 看看有多少数据解析抛出异常
    taxiParsed.map(_.isLeft).countByValue().foreach(println)

    // 3. 拿到有效的出租车载客记录数据，并转成Dataset
    // 数据parse之后类型是Either,取left.get拿到值
    // 拿到left之后类型是 Trip, 隐式转换，可以转成Dataset
    val taxiGood = taxiParsed.map(_.left.get).toDS
    taxiGood.cache()

    // 4. 对解析后的数据再次进行业务上异常数据的过滤
    // 比如，如果一次载客的时间超过了3个小时就不太正常，过滤掉
    // 4.1 计算乘客上车和下车的时间间隔的函数
    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    // 4.2 把函数注册成 udf, 且过滤掉一次载客的时间小于9小时或者超过3小时的记录
    val hoursUDF = udf(hours)
    taxiGood.groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

    // register the UDF, use it in a where clause
    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

    // 5. 准备纽约市每个区的地理位置信息
    // 5.1 读取 geoJson 数据
    val geojson = scala.io.Source.fromURL(this.getClass.getResource("/nyc-boroughs.geojson")).mkString

    // 5.2 将geoJson转成内存对象Feature
    val features = geojson.parseJson.convertTo[FeatureCollection]

    // 5.3 将feature按照区代吗和区的面积排序
    val areaSortedFeatures = features.sortBy { f =>
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())  // 美国的经度是负值，面积算出来是负的
    }

    // 5.4 将排序号的feature广播到每一个executor
    val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

    // 5.5 根据经度和纬度来定位所属区的方法，且将这个方法注册为udf
    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("borough").convertTo[String]
      }).getOrElse("NA")  // 找到了的话返回区的名字，没找到返回NA
    }
    val boroughUDF = udf(bLookup)

    // 5.6 过滤掉含有无效的经度和纬度载客的记录数据
    taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()
    val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")
    taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

    taxiGood.unpersist()

    // 6. 按照出租车进行重新分区，且在每一个分区中先按照出租车进行排序，然后再按照乘车上车的时间进行排序
    val sessions = taxiDone
      .repartition($"license")    // 做了一个类似于会话的计算，保证每个出租车司机的载客记录在同一个分区里面，保证在同一个executor里面进行计算
      .sortWithinPartitions($"license", $"pickupTime")  // 二次排序
      .cache()

    // 7. 计算一个出租车在一个区载客下车后，隔多长时间才载下一个载客
    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

    // 8. 计算所有出租车在一个区载客下车，隔多长时间才载下一个载客
    val boroughDurations: DataFrame =
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)  // 过滤掉只有一个的元素，和 license不相等的(不同车区间重叠的)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds")

    // 查看异常值
    boroughDurations
      .selectExpr("floor(seconds / 3600) as hours")
      .groupBy("hours")
      .count()
      .sort("hours")
      .show()

    // 9. 按照区进行聚合，计算出租车在每一个区载客下车后，隔多长时间才能载下一个乘客
    boroughDurations
      .where("seconds > 0 AND seconds < 60*60*4")
      .groupBy("borough")
      .agg(avg("seconds"), stddev("seconds"))   //标准差越小，平均值越准确
      .show()

    boroughDurations.unpersist()
  }

  /**
   * 通用的处理异常的方法, 出现异常程序不会终止
   * @param f   函数
   * @tparam S  函数f的输入类型
   * @tparam T  函数f的输出类型
   * @return    返回Either, Left为正常结果，Right为异常结果
   */
  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }
}
