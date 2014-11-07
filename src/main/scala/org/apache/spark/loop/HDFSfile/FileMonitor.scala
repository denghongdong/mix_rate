package org.apache.spark.loop.HDFSfile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import java.io.FileNotFoundException
import java.util.concurrent.LinkedBlockingQueue
import java.io.FileWriter
import org.apache.spark.loop.history.HistoryFileUtil
import org.apache.spark.Logging
import com.asiainfo.mix.rate.XmlAnalysis

/**
 * @author 宿荣全
 * @since 2014.09.22
 * ［HDFS粗粒度文件监控］功能解介：<br>
 * 1、对指定的ＨＤＦＳ路径以及该路径下的子文件夹作文件变更监控<br>
 * @param historyrddMap:File转换为rdd的历史记录拧成的hash 树；用于启动后从扫描路径中支除以前处理过的文件<br>
 * @param inputProp:the node [input]'s properties in core properties file. <br>
 * @param lbqHdfsFile:新增加的文件全部押在此队列中<br>
 * 注：此监控就文件级的，只对文件增减做监控，不对文件内容修改做监控<br>
 */
class FileMonitor(HDFSFileSytem: FileSystem, historyrddMap: Map[String, ArrayBuffer[String]],
  inputProp: Tuple2[String, Map[String, String]],
  lbqHdfsFile: LinkedBlockingQueue[(String, Array[String])]) extends Runnable with Logging {

  // logProperties 
  val topicLogType = inputProp._1
  val logProperties = inputProp._2
  val sourceHdfsPaht = logProperties("sourceHDFSPath")
  val toHdfsPath = logProperties("hdfsPath")
  // HDFS scanner
  val hdfsScannersMap = XmlAnalysis.getHdfsScannerMap
  val locallogpath = hdfsScannersMap("localLogDir")
  val scanInterval = (hdfsScannersMap("scanInterval")).toLong

  // overdueFile = true: overdue file is will be done.
  val overdueFile = true

  // 内存中维持的一个文件，文件夹关联的map
  //[folderpath_Name,(modify_time,fileList_hashcode,fileList)]
  val baseFolderMap = Map[String, (Long, Int, ArrayBuffer[String])]()
  //最新扫出的有变化的文件夹想关信息［只记录新增的文件夹和已有文件夹下文件的新增文件信息］
  val scanModifyFolderMap = Map[String, ArrayBuffer[String]]()

  // 记录新增文件列表
  var logwriter: FileWriter = null
  override def run() {
    // 创建log 生成器，记录新增文件
    logwriter = HistoryFileUtil.logWriter(locallogpath, topicLogType)
    if (logwriter == null) return
    val hpath = new Path(sourceHdfsPaht)

    require(keepHDFSpath(hpath), "This path is not exist in HDFS!")
    // 初始化加载baseFolderMap和scanModifyFolderMap
    initLoad(hpath)
    while (true) {
      folderMonitor
      fileNamePrint
      Thread.sleep(scanInterval)
      scanModifyFolderMap.clear
    }
  }

  /**
   * 打印结合项的值，供调试时使用
   */
  var hascodesMap = baseFolderMap.hashCode
  def fileNamePrint {
    if (hascodesMap != baseFolderMap.hashCode) {
      hascodesMap = baseFolderMap.hashCode
      //        println("-------------baseFolderMap-----------------------------")
      //        baseFolderMap.foreach(f => {
      //          val key = f._1
      //          val value = f._2
      //          println("folder:" + key)
      //          value._3 foreach println
      //        })
      //        println("-------------scanModifyFolderMap-----------------------")
      scanModifyFolderMap.foreach(f => {
        val key = f._1
        val value = f._2
        value.foreach(f => {
          logwriter.write(("新增文件：" + HistoryFileUtil.getNowTime + "	" + f + System.getProperty("line.separator")))
          logwriter.flush
        })
      })
    }
  }

  /**
   * 维护基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   * 1、gcBaseFolderMap：删除当前ＨＤＦＳ已经不存在的文件夹<br>
   * 2、维护现在的在ＨＤＦＳ上也切实存在的文件夹<br>
   */
  def folderMonitor {
    //删除已经移除的文件夹
    gcBaseFolderMap
    baseFolderMap.foreach(f => {
      try {
        val path = new Path(f._1)
        val scanfolderModifyTime = HDFSFileSytem.getFileStatus(path).getModificationTime()
        if (f._2._1 != scanfolderModifyTime) {
          fileScanner(path)
        }
      } catch {
        case ex: FileNotFoundException => ex.printStackTrace()
        case e: Exception => e.printStackTrace()
      }
    })
  }

  /**
   * 从内存中回收在ＨＤＦＳ中已不存在的那些路径
   */
  def gcBaseFolderMap {
    val baseArray = baseFolderMap.toArray
    val gcList = for (index <- 0 until baseArray.size if (!HDFSFileSytem.exists(new Path(baseArray(index)._1)))) yield (baseArray(index)._1)
    gcList.foreach(baseFolderMap.remove(_))
  }

  /**
   * baseFolderMap:总是保持指定目录下最新的新增文件结构<br>
   * scanModifyFolderMap:保存变更的文件部分<br>
   * 1、扫出新文件夹：将新建文件夹以及子文件夹，更新到基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   * 2、扫出新文件：置换baseFolderMap对应的key（文件夹名）的value，并把新增的文件追加到打描结果表［scanModifyFolderMap］<br>
   */
  def fileScanner(hdfsPath: Path) {
    try {

      // baseFolderInfo == null 说明是新增文件夹；新扫描的文件夹最后修改时间，跟对照表［baseFolderMap］中的修改时间不一致说明本目录中有文件增减
      val baseFolderInfo = baseFolderMap.getOrElse(hdfsPath.toString(), null)
      val scanfolderModifyTime = HDFSFileSytem.getFileStatus(hdfsPath).getModificationTime()

      val fileList = ArrayBuffer[String]()
      val status = HDFSFileSytem.listStatus(hdfsPath);
      status.foreach(f => {
        if (f.isDir) {
          // baseFolderInfo == null 说明是新增文件夹；
          if (baseFolderMap.getOrElse(f.getPath.toString, null) == null) fileScanner(f.getPath())
        } else {
          // acquire file node name from hdfs path.
          val hdfsFilePath = f.getPath.toString
          val nodeFile = hdfsFilePath.substring(hdfsFilePath.lastIndexOf(System.getProperty("file.separator")))
          fileList += nodeFile
        }
      })
      if (baseFolderInfo == null) {
        // 把新增文件压入队列
        val fullPathList = for (nodeFile <- fileList) yield (hdfsPath.toString + nodeFile)
        putQueue(fullPathList)
        // 更新扫描变更结果文件：scanModifyFolderMap
        scanModifyFolderMap += (hdfsPath.toString -> fileList)
        //重置此path下变更后的内容
        baseFolderMap += (hdfsPath.toString -> (scanfolderModifyTime, fileList.mkString.hashCode, fileList))
      } else {
        // 目录有文件增减
        // 文件列表hashcode有变化说明文件有增减
        if (baseFolderInfo._2 != fileList.mkString.hashCode) {
          // 文件列表的hashcode不一致说明是新增减了文件，相等说明是子目录的增加或子目录文件增加导致
          val basetArray = baseFolderInfo._3
          // 文件夹内有新增文件操作
          val addindex = for (file <- fileList if (!basetArray.contains(file))) yield (file)

          // 选出新增文件列表并把新增文件压入队列
          val fullPathList = for (nodeFile <- addindex) yield (hdfsPath.toString + nodeFile)
          putQueue(fullPathList)
          // 更新扫描变更结果文件：scanModifyFolderMap
          scanModifyFolderMap += (hdfsPath.toString -> fullPathList)
          //重置此path下变更后的内容
          baseFolderMap += (hdfsPath.toString -> (scanfolderModifyTime, fileList.mkString.hashCode, fileList))
        }
      }
    } catch {
      case e: Exception => logError("fileScanner:Exception!", e)
    }
  }

  /**
   * 把新增文件压入队列
   */
  def putQueue(list: ArrayBuffer[String]) {
    lbqHdfsFile.offer(toHdfsPath, list.toArray)
  }

  /**
   * 初始化加载，指定路径下完全扫描<br>
   * 扫描指定路径下的文件，以及子文件返回各文件夹的路径名称、最后修改时间、和文件列表。<br>
   * 装载基准表［baseFolderMap］和打描结果表［scanModifyFolderMap］<br>
   */
  def initLoad(hdfsPath: Path) {
    try {
      val folderInfo = HDFSFileSytem.getFileStatus(hdfsPath)
      val modifyTime = folderInfo.getModificationTime
      val fileList = ArrayBuffer[String]()
      val newAddFileList = ArrayBuffer[String]()
      val status = HDFSFileSytem.listStatus(hdfsPath);
      status.foreach(f => {
        if (f.isDir) initLoad(f.getPath()) else {
          // acquire file node name from hdfs path.
          val hdfsFilePath = f.getPath.toString
          val nodeFile = hdfsFilePath.substring(hdfsFilePath.lastIndexOf(System.getProperty("file.separator")))

          // 已有历史记录，非初次处理(重启操作)
          if (historyrddMap.size > 0) {
            // 拼接HDFS文件系统名称［hdfs://localhost:port］
            var hdpath = getHDFSFullName(hdfsPath)
            // 历史记录中没有处理过此条
            if (historyrddMap.getOrElse(hdpath, null) == null) {
              // 整个文件夹都没有
              fileList += nodeFile
              newAddFileList += nodeFile
            } else {
              val list = historyrddMap(hdpath)
              // 此文件夹下已有数据被处理过，但此文件未被处理
              if (!list.contains(nodeFile)) {
                newAddFileList += nodeFile
              }
              fileList += nodeFile
            }
          } else {
            // 初次启动
            fileList += nodeFile
            newAddFileList += nodeFile
          }
        }
      })
      baseFolderMap += (getHDFSFullName(hdfsPath) -> (modifyTime, fileList.mkString.hashCode, fileList))
      if (overdueFile) {
        // 把新增文件压入队列
        val fullPathList = for (nodeFile <- newAddFileList) yield (getHDFSFullName(hdfsPath) + nodeFile)
        putQueue(fullPathList)
        scanModifyFolderMap += (hdfsPath.toString -> newAddFileList)
      }
    } catch {
      case e: Exception => logError("initLoad:Exception!", e);
    }
  }

  /**
   * acquire hdfs file-path with HDFS file's prefixion.
   *  // 拼接HDFS文件系统名称［hdfs://localhost:port］
   */
  def getHDFSFullName(hdpath: Path) =
    if (!(hdpath.toString).startsWith("hdfs://")) (HDFSFileSytem.getUri()).toString + hdpath.toString
    else hdpath.toString
  /**
   * 确保HDFS上有此路径
   * @return true. 如果成功返回true
   */
  def keepHDFSpath(hdfsPath: Path): Boolean = {
    if (!HDFSFileSytem.exists(hdfsPath)) {
      logWarning(hdfsPath + " IN HDFS IS NOT EXISTED!")
      false
    } else {
      logInfo(hdfsPath + " IS LOADING!")
      true
    }
  }
}