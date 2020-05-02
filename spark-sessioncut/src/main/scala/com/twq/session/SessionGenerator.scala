package com.twq.session

import com.twq.spark.session.TrackerLog
import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ArrayBuffer

trait SessionGenerator {

  private val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 默认的会话切割逻辑
   * 每隔30s分钟切割会话
   * @param sortedTrackerLogs
   * @return
   */
  def cutSessions(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog] // 用于存放正在切割会话的日志
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]] // 用于存放切割完的会话所有的日志

    val cuttedSessionLogsResult: ArrayBuffer[ArrayBuffer[TrackerLog]] =
      sortedTrackerLogs.foldLeft((initBuilder, Option.empty[TrackerLog])) { case ((builder, preLog), currLog) =>
        val currLogTime = dateFormat.parse(currLog.getLogServerTime.toString).getTime
        // 如果当前的log与前一个log的时间超过30分钟的话，生成新的会话
        if (preLog.nonEmpty &&
          currLogTime - dateFormat.parse(preLog.get.getLogServerTime.toString).getTime >= 30 * 60 * 1000) {
          // 切割成一个新的会话
          builder += oneCuttingSessionLogs.clone()
          oneCuttingSessionLogs.clear()
        }
        // 把当前的log放到当前的会话里面
        oneCuttingSessionLogs += currLog
        (builder, Some(currLog))
      }._1.result()

    // 最后的会话添加
    if (oneCuttingSessionLogs.nonEmpty) {
      cuttedSessionLogsResult += oneCuttingSessionLogs
    }

    cuttedSessionLogsResult
  }
}

/**
 * 按照 pageview 进行会话切割
 */
trait PageViewSessionGenerator extends SessionGenerator {
  override def cutSessions(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog] // 用于存放正在切割会话的日志
    val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]] // 用于存放切割完的会话所有的日志

    val cuttedSessionLogsResult: ArrayBuffer[ArrayBuffer[TrackerLog]] =
      sortedTrackerLogs.foldLeft(initBuilder) { case (builder, currLog) =>
        // 如果当前的log是pageview的话，切割会话
        if (currLog.getLogType.toString.equals("pageview") && oneCuttingSessionLogs.nonEmpty) {
          // 切割成一个新的会话
          builder += oneCuttingSessionLogs.clone()
          oneCuttingSessionLogs.clear()
        }
        // 把当前的log放到当前的会话里面
        oneCuttingSessionLogs += currLog
        builder
      }.result()

    // 最后的会话添加
    if (oneCuttingSessionLogs.nonEmpty) {
      cuttedSessionLogsResult += oneCuttingSessionLogs
    }

    cuttedSessionLogsResult
  }
}
