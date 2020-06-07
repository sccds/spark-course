package com.twq.dataset.etl

import com.twq.dataset.Utils._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DeviceDataETL {

  def main(args: Array[String]): Unit = {
    val basePathKey = "spark.deviceDataETL.basePath"
    val config = new SparkConf()
    config.setAppName("DeviceDataETL")
    val isLocal = !config.contains(basePathKey)

    config.set("spark.debug.maxToStringFields", "10000")

    val basePath = if (isLocal) {
      config.setMaster("local")
      BASE_PATH
    } else config.get(basePathKey)

    val spark = SparkSession.builder()
      .config(config)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // json schema
    val deviceInfoSchema = new StructType()
      .add("devices",
        new StructType()
          .add("thermostats", MapType(StringType, // 温度调节装置
            new StructType()
              .add("device_id", StringType)
              .add("locale", StringType) // 场所
              .add("software_version", StringType) // 软件版本
              .add("structure_id", StringType) // 结构体id
              .add("where_name", StringType) // 所在位置的名称
              .add("last_connection", StringType) // 最后一次连接
              .add("is_online", BooleanType) // 是否在线
              .add("can_cool", BooleanType) // 是否可以制冷
              .add("can_heat", BooleanType) // 是否可以加热
              .add("is_using_emergency_heat", BooleanType) // 是否使用紧急加热
              .add("has_fan", BooleanType) // 是否有风扇
              .add("fan_timer_active", BooleanType) // 风扇定时器是否生效
              .add("fan_timer_timeout", StringType) // 风扇定时器超时时间
              .add("temperature_scale", StringType) // 温度单位
              .add("target_temperature_f", DoubleType) // 目标温度
              .add("target_temperature_high_f", DoubleType) // 目标最高温度
              .add("target_temperature_low_f", DoubleType) // 目标最低温度
              .add("eco_temperature_high_f", DoubleType) // eco最高温度
              .add("eco_temperature_low_f", DoubleType) // eco最低温度
              .add("away_temperature_high_f", DoubleType) // 离开最高温度
              .add("away_temperature_low_f", DoubleType) // 离开最低温度
              .add("hvac_mode", StringType) // 空调模式
              .add("humidity", LongType) // 湿度
              .add("hvac_state", StringType) // 空调状态
              .add("is_locked", StringType) // 是否被锁
              .add("locked_temp_min_f", DoubleType) // 锁定最低温度
              .add("locked_temp_max_f", DoubleType))) // 锁定最高温度
          .add("smoke_co_alarms", MapType(StringType,
            new StructType()
              .add("device_id", StringType)
              .add("locale", StringType)
              .add("software_version", StringType)
              .add("structure_id", StringType)
              .add("where_name", StringType)
              .add("last_connection", StringType)
              .add("is_online", BooleanType)
              .add("battery_health", StringType) // 电池健康状况
              .add("co_alarm_state", StringType) // 报警状态
              .add("smoke_alarm_state", StringType) // 烟雾报警状态
              .add("is_manual_test_active", BooleanType) // 人工调试是否生效
              .add("last_manual_test_active", StringType) // 最后人工调试的时间
              .add("ui_color_state", StringType))) // 洁面颜色状态
          .add("cameras", MapType(StringType,
            new StructType()
              .add("device_id", StringType)
              .add("software_version", StringType)
              .add("structure_id", StringType)
              .add("where_name", StringType)
              .add("is_online", BooleanType)
              .add("is_streaming", BooleanType) // 是否是流式数据
              .add("is_audio_input_enabled", BooleanType) // 声音输入是否开启
              .add("last_is_online_change", StringType) // 最后一次在线改变的时间
              .add("is_video_history_enabled", BooleanType) // 视频的历史记录是否开启
              .add("web_url", StringType)
              .add("app_url", StringType)
              .add("is_public_share_enabled", BooleanType) // 是否开启公共分享
              .add("activity_zones", // 活动区域
                new StructType()
                  .add("name", StringType)
                  .add("id", LongType)
              )
              .add("last_event", StringType)))) // 上次发送事件的时间

    import spark.implicits._

    val deviceInfoDF = spark
      .read
      .schema(deviceInfoSchema)
      .option("multiLine", true)
      .json(s"${basePath}/example/device_info_etl.json")

    deviceInfoDF.printSchema()
    deviceInfoDF.show()

    import org.apache.spark.sql.functions._

    val divideDeviceDF = deviceInfoDF.select(
      $"devices".getItem("smoke_co_alarms").alias("smoke_alarms"),
      $"devices".getItem("cameras").alias("cameras"),
      $"devices".getItem("thermostats").alias("thermostats")
    )
    divideDeviceDF.show()

    val explodedThermostatsDF = divideDeviceDF.select(explode($"thermostats"))
    val explodedCamerasDF = divideDeviceDF.select(explode($"cameras"))
    val explodedSmokedAlarmsDF = deviceInfoDF.select(explode($"devices.smoke_co_alarms"))

    val thermostateDF = explodedThermostatsDF.select(
      $"value".getItem("device_id").alias("device_id"),
      $"value".getItem("locale").alias("locale"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("last_connection").alias("last_connected"),
      $"value".getItem("humidity").alias("humidity"),
      $"value".getItem("target_temperature_f").alias("target_temperature_f"),
      $"value".getItem("hvac_mode").alias("mode"),
      $"value".getItem("software_version").alias("version")
    )

    thermostateDF.show()

    val cameraDF = explodedCamerasDF.select(
      $"value".getItem("device_id").alias("device_id"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("activity_zones").getItem("name").alias("name"),
      $"value".getItem("activity_zones").getItem("id").alias("id")
    )
    cameraDF.show()

    val smokedAlarmsDF = explodedSmokedAlarmsDF.select(
      $"value".getItem("device_id").alias("device_id"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("software_version").alias("version"),
      $"value".getItem("last_connection").alias("last_connected"),
      $"value".getItem("battery_health").alias("battery_health")
    )
    smokedAlarmsDF.show()

    spark.stop()

  }
}
