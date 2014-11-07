package com.asiainfo.mix.rate

import org.apache.spark.loop.history.HistoryFileCreate
import org.apache.spark.loop.HDFSfile.FileMonitor
import java.util.concurrent.LinkedBlockingQueue
import java.io.BufferedInputStream
import java.io.FileInputStream
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Calendar
import org.apache.spark.loop.history.HistoryFileUtil
import java.io.FileWriter

/**
 * @author surq
 * @since 2014.11.6
 * scan HDFS files and copy files.
 */
class HDFSCopy extends Runnable {
  // hdfs files handling start time.
  val xmlPropMap = XmlAnalysis.getHdfsScannerMap
  val filesstartTime = xmlPropMap("hdfsStartTime")
  val logStructMap = XmlAnalysis.getLogStructMap

  var historyRddWriter: FileWriter = _

  val conf = new Configuration()
  val HDFSFileSytem = FileSystem.get(conf)

  /**
   * start all file-scanner threads.
   */
  override def run = {

    // load last history file and created new history file. 
    val historyFileCreate = new HistoryFileCreate(HDFSFileSytem)
    historyFileCreate.doHistoryAction
    // from history screw to rdd-history-Map.
    val historyrddMap = historyFileCreate.getHistoryrddMap

    // and return history file writer
    historyRddWriter = historyFileCreate.getHistoryRddWriter
    val newFileQueue = new LinkedBlockingQueue[(String, Array[String])]()

    logStructMap.map(f => {
      new Thread(new FileMonitor(HDFSFileSytem, historyrddMap, f, newFileQueue)).start()
    })
    while (true) {
      val newFiles = newFileQueue.take()
      val desPath = newFiles._1
      val fileList = newFiles._2
      fileList.map(f => {
        if (fileFilter(f)) copyFileToHdfs(f, desPath)
      })
    }
  }

  /**
   * 复制HDFS 文件
   * @return true. 如果成功返回true
   */
  private def copyFileToHdfs(fromFile: String, toPath: String): Boolean = {
    val fileName = fromFile.substring(fromFile.lastIndexOf(System.getProperty("file.separator")) + 1)
    val tempName = System.getProperty("file.separator") + "_" + fileName
    val rename = toPath + System.getProperty("file.separator") + fileName
    val in = new BufferedInputStream(HDFSFileSytem.open(new Path(fromFile)))
    val out = HDFSFileSytem.create(new Path(toPath + tempName))
    var return_flg = true
    try {
      IOUtils.copyBytes(in, out, 4096, false)
      return_flg = true
      return_flg
    } catch {
      case e: Exception => e.printStackTrace(); return_flg = false; return_flg
    } finally {
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
      try {
        if (return_flg) {
          HDFSFileSytem.rename(new Path(toPath + tempName), new Path(rename))
          historyRddWriter.write(fromFile + System.getProperty("line.separator"))
          historyRddWriter.flush
        } else HDFSFileSytem.delete(new Path(toPath + tempName))
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * file filter
   */
  def fileFilter(filePath: String) = {
    val fileName = filePath.substring(filePath.lastIndexOf(System.getProperty("file.separator")) + 1)
    if (fileName.startsWith("_") || fileName.startsWith(".") || fileName.endsWith("._COPYING_")) false else true
  }
  // if (fileName.matches(".*(._COPYING_)$") || fileName.matches("/[_|.].*")) false else true
}