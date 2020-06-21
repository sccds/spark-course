package com.twq.parser

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.Row

case class Trip(
               license: String,     // 出租车牌号
               pickupTime: Long,    // 乘客上车时间
               dropoffTime: Long,   // 乘客下车时间
               pickupX: Double,     // 乘客上车时间的经度
               pickupY: Double,     // 乘客上车时间的纬度
               dropoffX: Double,    // 乘客下车时间的经度
               dropoffY: Double)     // 乘客下车时间的纬度

/**
 * 不会出现空指针异常的Row
 * @param row 原始DataFrame中的Row
 */
class RichRow(row: Row) {

  /**
   * 根据字段名称从Row中获取对应的值
   * @param field 字段名
   * @tparam T    返回值类型
   * @return  值，如果字段名不存在或者字段名对应的值为null的话返回None，否则返回Some(值)
   */
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
}


object TaxiTripParser {

  /**
   * 解析从csv中读取出来的每一行载客记录
   *
   * @param row
   * @return
   */
  def parse(row: Row): Trip = {
    val rr = new RichRow(row)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
    )
  }

  // 获取乘客上下的时间并且格式化时间
  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  // 获取乘客上下车的时间别切
  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }
}


