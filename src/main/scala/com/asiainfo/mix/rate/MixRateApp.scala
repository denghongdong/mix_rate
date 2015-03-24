package com.asiainfo.mix.rate

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar

/**
 * @author surq
 * @since 2014.10.28
 * 功能解介：<br>
 * 根据配置文件定义的维度，统计出达到一个阀值时的各累计字段的值。<br>
 */
object MixRateApp extends Serializable {

  def main(args: Array[String]): Unit = {

    require(args.size == 1, "Please input the flg that is  Whether run copy files script. [true]:yes ,[false]:no")
    XmlAnalysis.annalysis("conf/logconf.xml")
    // -----------------------------------------------
    // 是否运行拷贝文件脚本
    val copy_flg = args(0).toBoolean
    if (copy_flg) new Thread(new HDFSCopy()).start()
    // -----------------------------------------------

    val AppPropertiesMap = XmlAnalysis.getAppPropertiesMap
    val logStructMap = XmlAnalysis.getLogStructMap
    val httpMap = XmlAnalysis.getHttpMap

    val separator = AppPropertiesMap("separator")
    val totalOutPutItem = (AppPropertiesMap("msgStruct").split(",")).zip(AppPropertiesMap("msgItems").split(","))
    val appName = AppPropertiesMap("appName")
    val streamSpace = AppPropertiesMap("interval")
    val output_prefix = AppPropertiesMap("output_prefix")
    val output_suffix = AppPropertiesMap("output_suffix")
    val expose_threshold = AppPropertiesMap("expose_threshold")
    val checkpointPath = AppPropertiesMap("checkpointPath")
    // TODO
        val master = "local[2]"
        val ssc = new StreamingContext(master, appName, Seconds(streamSpace.toInt))

//    val sparkConf = new SparkConf().setAppName(appName)
//    val ssc = new StreamingContext(sparkConf, Seconds(streamSpace.toInt))

    ssc.checkpoint(checkpointPath)
    val dstreamList = logStructMap.map(log => {
      val propMap = log._2
      val appClass = propMap("appClass")
      val hdfsPath = propMap("hdfsPath")
      val items = propMap("items")
      val itemszip = zipKeyValue(separator, items)
      val stream = ssc.textFileStream(hdfsPath).filter(valadityCheck(separator, _)).map(itemszip)
      val clz = Class.forName(appClass)
      val constructors = clz.getConstructors()
      val constructor = constructors(0).newInstance()
      val outputStream = constructor.asInstanceOf[StreamAction].run(stream, propMap)
      outputStream
    })

    // union handle
    val setThreshold = updateFunc(expose_threshold.toInt)
    val dataFormatBykey = setOutPutItems(totalOutPutItem)
    val responseSend = sethttpParam(httpMap.toArray)

    val joinsDstreams = dstreamList.reduceLeft(_ union _).updateStateByKey(setThreshold).map(dataFormatBykey).mapPartitions(responseSend)
    joinsDstreams.saveAsTextFiles(output_prefix, output_suffix)
    
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * send http response by partition.
   */
  def sethttpParam(dataArray: Array[(String, String)]) = (iter: Iterator[String]) => {
    val res = ArrayBuffer[String]()
    iter.foreach(res += _)
    val st = res.toArray
    HttpUtil.msgSend(dataArray, st)
    res.iterator
  }

  /**
   *  valid data check
   */
  def valadityCheck(separator: String, contentText: String): Boolean = {
    if (contentText.trim == "") {
      println("空数据")
      false
    } else {
      val valueList = valueToList(separator, contentText)
      if (valueList(0).matches("[1-9][0-9]*") && valueList.size == valueList(0).toInt) {
        true
      } else {
        println("Invalid Data[残缺数据]:" + contentText)
        false
      }
    }
  }

  /**
   * each line of file to list and return.
   */
  def valueToList(separator: String, contentText: String) = {
    val data = contentText + separator + "MixSparkLogEndSeparator"
    val recodeList = data.split(separator)
    for (index <- 0 until recodeList.size - 1) yield (recodeList(index))
  }

  /**
   * key zip value (key:item,value:item of line)
   */
  def zipKeyValue(separator: String, items: String) = (contentText: String) => {
    val valueList = valueToList(separator, contentText)
    (items.split(",")).zip(valueList)
  }

  /**
   * The summation of each field.
   * example: bid.[count]:1000,expose.[count]:2000,expose.price:4000.0,click.[count]:3000
   */
  def setOutPutItems(totalOutPutItem: Array[(String, String)]) = (resultSet: Tuple2[String, Tuple2[Map[String, Double], Queue[Map[String, Double]]]]) => {
    val key = resultSet._1
    val outputMap = (resultSet._2)._1

    val resut = for (item <- totalOutPutItem) yield {
      var value = ""
      if ((item._1).toLowerCase == "[rowkey]") value = key
      else value = (outputMap.getOrElse(item._1, 0d)).toString
      // [count] as key , transform value's type double to int.
      if ((item._1).matches(""".*(\[count\])$""")) item._2 + ":" + value.substring(0, value.indexOf("."))
      else item._2 + ":" + value
    }
    resut.mkString(",")
  }

  /**
   * updateStateByKey
   */
  def updateFunc(expose_threshold: Int) = (values: Seq[String], state: Option[(Map[String, Double], Queue[Map[String, Double]])]) => {
    val stateStruct = state.getOrElse(Tuple2(Map[String, Double](), new Queue[Map[String, Double]]()))
    val keepMap = stateStruct._1
    val stateQueue = stateStruct._2

    val currentMap = Map[String, Double]()
    for (each <- values; item <- each.split("\\|")) {
      val value = item.trim.split(":")
      currentMap += (value(0) -> (currentMap.getOrElse(value(0), 0d) + value(1).toDouble))
      keepMap += (value(0) -> (keepMap.getOrElse(value(0), 0d) + value(1).toDouble))
    }
    if (!currentMap.isEmpty) {
      // put current batch bata into stateQueue.
      stateQueue.enqueue(currentMap)
      var overValue = keepMap.getOrElse("expose.[count]", 0d) - expose_threshold
      while (overValue > 0) {
        val subtractValue = overValue - stateQueue(0).getOrElse("expose.[count]", 0d)
        if (subtractValue > 0) {
          overValue = subtractValue
          val dropMap = stateQueue.dequeue
          dropMap foreach { enm =>
            {
              keepMap.update(enm._1, keepMap(enm._1) - enm._2)
            }
          }
        }
      }
    }
    Some((keepMap, stateQueue))
    state
  }
}