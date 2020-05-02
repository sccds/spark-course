package com.twq.session

import java.util.concurrent.TimeUnit

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * 会话切割项目程序入口
 */
object SessionCutETL {

  private val logTypeSet = Set("pageview", "click")

  private val logger = LoggerFactory.getLogger("SessionCutETL")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SessionCutETL")
    if (!conf.contains("spark.master")) {
      conf.setMaster("local") // 用本地跑
    }

    // 开启 kryo 序列化机制
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    // 通过配置，拿到配置的输入和输出路径, 一定得是spark开头的才能认
    val visitLogsInputPath = conf.get("spark.sessioncut.visitLogsInputPath", "data/rawdata/visit_log.txt")
    val cookieLabelInputPath = conf.get("spark.sessioncut.cookieLabelInputPath", "data/cookie_label.txt")
    val baseOutputPath = conf.get("spark.sessioncut.baseOutputPath", "data/output")

    // 参数也可以通过 main 方法里面的传参
    val outputFileType = if (args.nonEmpty) args(0) else "text"

    logger.info(
      s"""Starting SessionCutETL with visitLogsInputPath: ${visitLogsInputPath},
         |cookieLabelInputPath: ${cookieLabelInputPath},
         |baseOutputPath: ${baseOutputPath}
         |outputFileType: ${outputFileType}""".stripMargin)

    // 网站域名标签map, 可以放在数据库中，然后从数据库中捞取出来
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    val domainLabelMapB = sc.broadcast(domainLabelMap)

    // 统计会话的个数
    val sessionCountAcc = sc.longAccumulator("session count")

    // 1. 加载数据 (visit_log.txt)
    val rawRDD: RDD[String] = sc.textFile(visitLogsInputPath)

    // 2. 解析rawRDD中的每一行原始日志
    val parsedLogRDD: RDD[TrackerLog] = rawRDD
      .flatMap(RawLogParser.parse)
      .filter(trackerLog => logTypeSet.contains(trackerLog.getLogType.toString))

    // 优化，对于使用多次的RDD进行cache
    parsedLogRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 3. 按照 cookie 进行分组，生成 userGroupedRDD
    val userGroupedRDD: RDD[(String, Iterable[TrackerLog])] = parsedLogRDD.groupBy(trackerLog => trackerLog.getCookie.toString)

    // 4. 对每一个user(即每一个cookie)的所有的日志进行会话切割
    val sessionRDD: RDD[(String, TrackerSession)] = userGroupedRDD.flatMapValues { case iter =>
      // user级别的日志处理
      val processor = new OneUserTrackerLogsProcessor(iter.toArray) with PageViewSessionGenerator // 混入另一个特质，调用PageViewSessionGenerator
      processor.buildSessions(domainLabelMapB.value, sessionCountAcc)
    }

    // 5. 给会话的cookie打上标签
    val cookieLabelRDD: RDD[(String, String)] = sc.textFile(cookieLabelInputPath).map { case line =>
      val temp = line.split("\\|")
      (temp(0), temp(1))  // (cookie, cookie_label）
    }

    val joinRDD: RDD[(String, (TrackerSession, Option[String]))] =
      sessionRDD.leftOuterJoin(cookieLabelRDD)

    val cookieLabeledSessionRDD: RDD[TrackerSession] = joinRDD.map { case (_, (session, cookieLabelOpt)) =>
      if (cookieLabelOpt.nonEmpty) {
        session.setCookieLabel(cookieLabelOpt.get)
      } else {
        session.setCookieLabel("-")
      }
      session
    }

    cookieLabeledSessionRDD.collect().foreach(println)

    // 6. 保存结果数据
    OutputComponent.fromOutputFileType(outputFileType)
      .writeOutputData(sc, baseOutputPath, parsedLogRDD, cookieLabeledSessionRDD)

    //TimeUnit.SECONDS.sleep(100)

    sc.stop()

    logger.info("end SessionCutETL")
  }
}
