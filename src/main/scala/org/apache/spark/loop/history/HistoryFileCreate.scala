package org.apache.spark.loop.history

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.beans.BeanProperty
import java.io.FileWriter
import com.asiainfo.mix.rate.XmlAnalysis
import org.apache.spark.Logging
import org.apache.hadoop.fs.FileSystem

class HistoryFileCreate(HDFSFileSytem: FileSystem) extends Logging{

  @BeanProperty var historyrddMap = Map[String, ArrayBuffer[String]]()
  @BeanProperty var historyRddWriter: FileWriter = _
  val properiesMap = XmlAnalysis.getHdfsScannerMap
  // 本地文件日志
  val locallogpath = properiesMap("localLogDir")
  /**
   * update laste history file:
   * 1、iterate last history file.
   * 2、remove no exist files in current HDFS system.
   * 3、crteate and write new histoy file.
   */
  def doHistoryAction = {
    iteratorHistory
    // 去除历史记录文件中HDFS文件系统中已不存在的文件
    HistoryFileUtil.del_Hdfs_NotExist(HDFSFileSytem,historyrddMap)
    historyWrite
    logInfo("spark Streaming最新处理历史记录已更新完成！")
  }

  /**
   * load history file to historyrddMap
   */
  def iteratorHistory = {
    //  加载rddfile历史记录，rdd历史记录文件宁成的hash树
    val historyInfo = HistoryFileUtil.historyRddAnalysis(locallogpath)
    // 最新历史记录文件路径
    val historyFile = historyInfo._1
    println("历史处理文件加载[INFO:]" + historyFile)
    // 最新历史记录文件拧成的hash路径树
    historyrddMap = historyInfo._2
  }

  /**
   * creade FileWriter and write old history-file.
   */
  def historyWrite = {
    // 重新生成历史记录文件列表
    historyRddWriter = HistoryFileUtil.rddFileHistoryWriter(locallogpath)
    // 已有历史记录写入新的记录
    historyrddMap.map(f => {
      f._2.foreach(each => {
        historyRddWriter.write(f._1 + each + System.getProperty("line.separator"))
        historyRddWriter.flush
      })
    })
  }
}