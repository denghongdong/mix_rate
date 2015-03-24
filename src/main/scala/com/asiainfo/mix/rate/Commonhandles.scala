package com.asiainfo.mix.rate

import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * @author surq
 * @since 2014.10.30
 * 共通函数<br>
 */
object Commonhandles {

  /**
   * compute output items's count.
   */
  def compute(inputStream: DStream[Array[(String, String)]], propMap: Map[String, String]): DStream[(String, String)] = {
    val groupKey = propMap("groupByKey").split(",")
    val topicLogType = propMap("topicLogType").trim
    val outputItems = propMap("outputItems").split(",")
    val rokeyData = Commonhandles.groupkeyEdit(groupKey)
    val doCompute = Commonhandles.sumItems(topicLogType, outputItems)
    inputStream.map(rokeyData).map(doCompute)
  }

  /**
   * According to the Provided groupKey to output (k/v).
   */
  def groupkeyEdit(groupKey: Array[String]) = (input: Array[(String, String)]) => {
    val keyMap = input.toMap
    val rowKey = (for (key <- groupKey) yield (keyMap(key))).mkString
    (rowKey, input)
  }
  
  /**
   * According to the same primary key to compute record's output Items total count.
   */
  def sumItems(topicLogType: String, outputItems: Array[String]) = (recordData: Tuple2[String, Array[(String, String)]]) => {
    
    val recordMap = recordData._2.toMap
    val resultMap = Map[String, String]()
    outputItems.foreach(f => {
      val outItem = f.trim
      if (outItem == "[count]") {
        resultMap += (outItem -> "1")
      } else {
        resultMap += (outItem -> recordMap.getOrElse(outItem, "0"))
      }
    })
    // example: bid.[count]:2000|bid.xxx:3000
    val result = (for (item <- outputItems) yield (topicLogType + "." + item + ":" + resultMap(item))).mkString("|")
    (recordData._1, result)
  }
}