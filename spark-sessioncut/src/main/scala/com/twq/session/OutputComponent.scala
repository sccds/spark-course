package com.twq.session

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 输出组件
 */
trait OutputComponent {

  /**
   * 保存结果数据的方法
   * @param sc
   * @param baseOutputPath
   * @param parsedLogRDD
   * @param cookieLabeledSessionRDD
   */
  def writeOutputData(sc: SparkContext, baseOutputPath: String,
                      parsedLogRDD: RDD[TrackerLog], cookieLabeledSessionRDD: RDD[TrackerSession]) = {
    deleteIfExist(sc, baseOutputPath)

  }

  private def deleteIfExist(sc: SparkContext, trackerLogOutputPath: String) = {
    val path = new Path(trackerLogOutputPath)
    val fileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true) // true 递归删除
    }
  }
}

/**
 * 写 parquet 文件
 */
class ParquetFileOutput extends OutputComponent {
  /**
   * 保存结果数据的方法
   *
   * @param sc
   * @param baseOutputPath
   * @param parsedLogRDD
   * @param cookieLabeledSessionRDD
   */
  override def writeOutputData(sc: SparkContext, baseOutputPath: String,
                               parsedLogRDD: RDD[TrackerLog],
                               cookieLabeledSessionRDD: RDD[TrackerSession]): Unit = {
    // deleteIfExist 通用方法，直接在父类中实现，子类调用的时候直接继承父类
    super.writeOutputData(sc, baseOutputPath, parsedLogRDD, cookieLabeledSessionRDD)

    // 6.1. 保存 TrackerLog --> parsedLogRDD
    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
    parsedLogRDD.map((null, _)).saveAsNewAPIHadoopFile(trackerLogOutputPath, classOf[Void],
      classOf[TrackerLog], classOf[AvroParquetOutputFormat[TrackerLog]])

    // 6.2 保存 TrackerSession --> cookieLabeledSessionRDD
    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerSession.SCHEMA$)
    cookieLabeledSessionRDD.map((null, _)).saveAsNewAPIHadoopFile(trackerSessionOutputPath, classOf[Void],
      classOf[TrackerSession], classOf[AvroParquetOutputFormat[TrackerSession]])
  }

}

/**
 * 写 textfile
 */
class TextFileOutput extends OutputComponent {
  /**
   * 保存结果数据的方法
   *
   * @param sc
   * @param baseOutputPath
   * @param parsedLogRDD
   * @param cookieLabeledSessionRDD
   */
  override def writeOutputData(sc: SparkContext, baseOutputPath: String,
                               parsedLogRDD: RDD[TrackerLog],
                               cookieLabeledSessionRDD: RDD[TrackerSession]): Unit = {

    super.writeOutputData(sc, baseOutputPath, parsedLogRDD, cookieLabeledSessionRDD)

    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
    parsedLogRDD.saveAsTextFile(trackerLogOutputPath)

    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerSession.SCHEMA$)
    cookieLabeledSessionRDD.saveAsTextFile(trackerSessionOutputPath)
  }
}

/**
 * 想要对外提供服务，写一个工厂方法，使用伴生对象
 */
object OutputComponent {
  def fromOutputFileType(fileType: String): OutputComponent = {
    if (fileType.equals("parquet")) {
      new ParquetFileOutput
    } else {
      new TextFileOutput
    }
  }
}