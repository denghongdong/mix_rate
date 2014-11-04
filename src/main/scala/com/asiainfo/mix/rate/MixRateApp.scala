package com.asiainfo.mix.rate

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import scala.collection.mutable.Queue
import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2014.10.28
 * 功能解介：<br>
 * 根据配置文件定义的维度，统计出达到一个阀值时的各累计字段的值。<br>
 */
object MixRateApp extends Logging {

  var separator = ""
  var totalOutPutItem:Array[String] = _
  
  def main(args: Array[String]): Unit = {

    XmlAnalysis.annalysis("conf/logconf.xml")
    val AppPropertiesMap = XmlAnalysis.getAppPropertiesMap
    val logStructMap = XmlAnalysis.getLogStructMap

    separator = AppPropertiesMap("separator")
    // 各输出字段加前辍［logtype.item］
    totalOutPutItem = (for (log <- logStructMap; logtype = log._1) yield {
      val outputItemList = ((log._2("outputItems")).split(",")).map(f => logtype + "." + f)
      outputItemList.mkString(",")
    }).mkString(",").split(",")

    val appName = AppPropertiesMap("appName")
    val streamSpace = AppPropertiesMap("interval")
    val output_prefix = AppPropertiesMap("output_prefix")
    val output_suffix = AppPropertiesMap("output_suffix")
    val expose_threshold = AppPropertiesMap("expose_threshold")
    val checkpointPath = AppPropertiesMap("checkpointPath")
    val setThreshold = updateFunc(expose_threshold.toInt)

    // TODO
    val master = "local[2]"
    val ssc = new StreamingContext(master, appName, Seconds(streamSpace.toInt), System.getenv("SPARK_HOME"))

    //    val sparkConf = new SparkConf().setAppName(appName)
    //    val ssc = new StreamingContext(sparkConf, Seconds(streamSpace.toInt))

    ssc.checkpoint(checkpointPath)
    val dstreamList = logStructMap.map(log => {
      val propMap = log._2
      val appClass = propMap("appClass")
      val hdfsPath = propMap("hdfsPath")
      val items = propMap("items")
      val itemszip = zipKeyValue(items)
      val stream = ssc.textFileStream(hdfsPath).filter(valadityCheck).map(itemszip)
      val clz = Class.forName(appClass)
      val constructors = clz.getConstructors()
      val constructor = constructors(0).newInstance()
      val outputStream = constructor.asInstanceOf[StreamAction].run(stream, propMap)
      outputStream
    })

    // union handle
    val joinsDstreams = dstreamList.reduceLeft(_ union _).updateStateByKey(setThreshold).map(dataMergeBykey)
    joinsDstreams.saveAsTextFiles(output_prefix, output_suffix)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   *  valid data check
   */
  def valadityCheck(contentText: String): Boolean = {
    if (contentText.trim == "") {
      logWarning("空数据")
      false
    } else {
      val valueList = valueToList(contentText)
      if (valueList(0).matches("[1-9][0-9]*") && valueList.size == valueList(0).toInt) true else {
        logWarning("Invalid Data[残缺数据]:" + contentText)
        false
      }
    }
  }

  /**
   * each line of file to list and return.
   */
  def valueToList(contentText: String) = {
    var data = contentText + separator + "MixSparkLogEndSeparator"
    val recodeList = data.split(separator)
    for (index <- 0 until recodeList.size - 1) yield (recodeList(index))
  }

  /**
   * key zip value (key:item,value:item of line)
   */
  def zipKeyValue(items: String) = (contentText: String) => {
    val valueList = valueToList(contentText)
    (items.split(",")).zip(valueList)
  }

  /**
   * two dstreams Union to one dstream.<br>
   * value: each of dstream is separated by "|"<br>
   */
//  val dstreamsUnion = (d1: DStream[(String, String)], d2: DStream[(String, String)]) => d1.union(d2)

  /**
   * The summation of each field.
   * example: bid.[count]:1000,expose.[count]:2000,expose.price:4000.0,click.[count]:3000
   */
  def dataMergeBykey(resultSet: Tuple2[String, Queue[Map[String, Double]]]) = {
    val resultMap = Map[String, Double]()
    val key = resultSet._1
    val queueSets = resultSet._2
    for (map <- queueSets; item <- totalOutPutItem) {
      resultMap += (item -> ((map.getOrElse(item, 0d)) + (resultMap.getOrElse(item, 0d))))
    }
    val resut = for (item <- totalOutPutItem) yield {
      val value = (resultMap.getOrElse(item, 0d)).toString
      if (item.matches(""".*(\[count\])$""")) {
        item + ":" + value.substring(0, value.indexOf("."))
      } else {
        item + ":" + value
      }
    }
    key + "	" + resut.mkString(",")
  }
  
  /**
   * updateStateByKey
   */
  def updateFunc(expose_threshold: Int) = (values: Seq[String], state: Option[Queue[Map[String, Double]]]) => {

    val currentMap = Map[String, Double]()
    for (each <- values; item <- each.split("\\|")) {
      val value = item.trim.split(":")
      currentMap += (value(0) -> (currentMap.getOrElse(value(0), 0d)+ value(1).toDouble))
    }
    val stateQueue = state.getOrElse(new Queue[Map[String, Double]]())
    stateQueue.enqueue(currentMap)
    var sum = 0d
    var index = stateQueue.size - 1
    while (sum < expose_threshold && index >= 0) {
      val maptmp = stateQueue(index)
      val exposeCunt = (maptmp.getOrElse("expose.[count]", 0d))
      sum = sum + exposeCunt
      index = index - 1
    }
    // form Queue move
    0 to index foreach (f => { stateQueue.dequeue })
    Some(stateQueue)
  }
}