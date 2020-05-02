package com.twq.session

import com.twq.spark.session.TrackerLog

object RawLogParser {

  /**
   * 将每一行的原始日志转成 TrackerLog 对象
   * @param line 原始日志
   * @return TrackerLog对象
   */
  def parse(line: String): Option[TrackerLog] = {
    if (line.startsWith("#")) None
    else {
      val fields = line.split("\\|")
      val trackerLog = new TrackerLog()
      trackerLog.setLogType(fields(0))
      trackerLog.setLogServerTime(fields(1))
      trackerLog.setCookie(fields(2))
      trackerLog.setIp(fields(3))
      trackerLog.setUrl(fields(4))
      Some(trackerLog)
    }
  }
}
