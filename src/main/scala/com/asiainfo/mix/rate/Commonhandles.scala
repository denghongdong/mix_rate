package com.asiainfo.mix.rate

import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import scala.beans.BeanProperty

/**
 * @author surq
 * @since 2014.10.30
 * 共通函数<br>
 */
object Commonhandles extends Logging {

  XmlAnalysis.annalysis("conf/logconf.xml")
  def main(sfsf: Array[String]) = {

    val AppPropertiesMap = XmlAnalysis.getAppPropertiesMap
    val logStructMap = XmlAnalysis.getLogStructMap

    //      val totalOutPutItem = ((for (log <- logStructMap) yield ((log._2)("outputItems"))).mkString(",")).split(",")
    val totalOutPutItem = (for (log <- logStructMap; logtype = log._1) yield {
      val outputItemList = ((log._2("outputItems")).split(",")).map(f => logtype + "." + f)
      outputItemList.mkString(",")
    }).mkString(",").split(",")

    totalOutPutItem foreach println

  }

  private val appPrpMap = XmlAnalysis.getAppPropertiesMap
  private val separator = appPrpMap("separator")
  private val logStructMap = XmlAnalysis.getLogStructMap

  // 各输出字段加前辍［logtype.item］
  val totalOutPutItem = (for (log <- logStructMap; logtype = log._1) yield {
    val outputItemList = ((log._2("outputItems")).split(",")).map(f => logtype + "." + f)
    outputItemList.mkString(",")
  }).mkString(",").split(",")

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
   * compute output items's count.
   */
  def compute(inputStream: DStream[Array[(String, String)]], propMap: Map[String, String]): DStream[(String, String)] = {
    val groupKey = propMap("groupByKey").split(",")
    val topicLogType = propMap("topicLogType").trim
    val outputItems = propMap("outputItems").split(",")
    val rokeyData = Commonhandles.groupkeyEdit(groupKey, topicLogType)
    val doCompute = Commonhandles.sumItems(topicLogType, outputItems)
    inputStream.map(rokeyData).groupByKey.map(doCompute)
  }

  //---------------------------------------------- typey 1: join start --------------------------
  /**
   * two dstreams join to one dstream.<br>
   * value: each of dstream is separated by "|"<br>
   */
  def dstreamsJoin(d1: DStream[(String, String)], d2: DStream[(String, String)]): DStream[(String, String)] = {
    val mergeDstream = d1.join(d2)
    mergeDstream.map(f => {
      val joinValue = f._2
      val value = joinValue._1 + "|" + joinValue._2
      (f._1, value)
    })
  }

  //---------------------------------------------- typey 2: union  --------------------------
  /**
   * two dstreams Union to one dstream.<br>
   * value: each of dstream is separated by "|"<br>
   */
  val dstreamsUnion = (d1: DStream[(String, String)], d2: DStream[(String, String)]) => d1.union(d2)

  /**
   * updateStateByKey
   */

  def updateFunc(expose_threshold: Int) = (values: Seq[String], state: Option[Queue[Map[String, Double]]]) => {

    val currentMap = Map[String, Double]()
    for (each <- values; item <- each.split("\\|")) {
      val value = item.trim.split(":")
      currentMap += (value(0) -> value(1).toDouble)
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
  //---------------------------------------------- typey 2: end  --------------------------
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
   * According to the Provided groupKey to output (k/v).
   */
  def groupkeyEdit(groupKey: Array[String], topicLogType: String) = (input: Array[(String, String)]) => {
    val keyMap = input.toMap
    val rowKey = (for (key <- groupKey) yield (keyMap(key))).mkString
    (rowKey, input)
  }

  /**
   * According to the same primary key to compute record's output Items total count.
   */
  def sumItems(topicLogType: String, outputItems: Array[String]) = (dataSet: Tuple2[String, Seq[Array[(String, String)]]]) => {
    var resultMap = Map[String, Double]()
    outputItems.foreach(f => {
      val outItem = f.trim
      if (outItem == "[count]") {
        resultMap += (outItem -> (dataSet._2.size).toDouble)
      } else {
        (dataSet._2).foreach(eatchArray => {
          val kv = eatchArray.toMap
          resultMap += (outItem -> (resultMap.getOrElse(outItem, 0d) + kv(outItem).toDouble))
        })
      }
    })
    // example: bid.[count]:2000|bid.xxx:3000
    val result = (for (item <- outputItems) yield (topicLogType + "." + item + ":" + resultMap(item))).mkString("|")
    logWarning("主key:" + dataSet._1 + " value:" + result)
    (dataSet._1, result)
  }
}