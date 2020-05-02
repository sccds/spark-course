package com.twq.session

import java.net.URL
import java.util.UUID

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

/**
 * 处理单个user下面所有的trackerLogs
 * @param trackerLogs 单个user的trackerLogs
 */
class OneUserTrackerLogsProcessor(trackerLogs: Array[TrackerLog]) extends SessionGenerator {

  private val sortedTrackerLogs = trackerLogs.sortBy(_.getLogServerTime.toString)

  /**
   * 生成当前这个user下的所有的会话
   * @return
   */
  def buildSessions(domainLabelMap: Map[String, String],
                    sessionCountAcc: LongAccumulator): ArrayBuffer[TrackerSession] = {
    // 1. 会话切割
    val cuttedSessionLogsResult = cutSessions(sortedTrackerLogs)

    // 2. 生成会话
    // 如果用 pageview, 第一次来的时候，cuttedSessionLogsResult为空
    cuttedSessionLogsResult.map { case sessionLogs =>
      val session = new TrackerSession()
      session.setSessionId(UUID.randomUUID().toString)
      session.setSessionServerTime(sessionLogs.head.getLogServerTime)
      session.setCookie(sessionLogs.head.getCookie)
      session.setIp(sessionLogs.head.getIp)

      val pageViewLogs = sessionLogs.filter(_.getLogType.equals("pageview"))
      if (pageViewLogs.length == 0) {
        session.setLandingUrl("-")
      } else {
        session.setLandingUrl(pageViewLogs.head.getUrl)
      }

      session.setPageviewCount(pageViewLogs.length)

      val clickLogs = sessionLogs.filter(_.getLogType.equals("click"))
      session.setClickCount(clickLogs.length)

      if (pageViewLogs.length == 0) {
        session.setDomain("-")
      } else {
        val url = new URL(pageViewLogs.head.getUrl.toString)
        session.setDomain(url.getHost)
      }

      val domainLabel = domainLabelMap.getOrElse(session.getDomain.toString, "-")
      session.setDomainLabel(domainLabel)

      // 统计会话个数
      sessionCountAcc.add(1)

      session
    }
  }


}
