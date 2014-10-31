package com.asiainfo.mix.rate.impl

import com.asiainfo.mix.rate.StreamAction
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import com.asiainfo.mix.rate.Commonhandles
import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2014.10.28
 * 功能解介：<br>
 * 竞价日志处理逻辑<br>
 */
class Bid extends StreamAction with Serializable{
  override def run(inputStream: DStream[Array[(String, String)]], propMap: Map[String, String]): DStream[(String, String)] = {
    val topicLogType = propMap("topicLogType")
    val groupKey = propMap("groupByKey").split(",")
    val outputItems = propMap("outputItems").split(",")
    val doCompute = Commonhandles.sumItems(topicLogType, outputItems)
    val setGroupKey = groupKeyOut(topicLogType, groupKey)
    val inputDstream = inputStream.filter(bidFilter).map(setGroupKey).groupByKey.map(doCompute)
    inputDstream
  }

  /**
   * 目前日志中混入了无线端的数据，请在前期不解析移动端的（因为数据结构略有不同）<br>
   * 在取数据时加入“exchange_id=1”的过滤条件，就会只取百度adx的数据。<br>
   */
  private def bidFilter(record: Array[(String, String)]): Boolean = {
    val valueMap = record.toMap
    if ((valueMap.getOrElse("exchange_id", "null")).trim == "1") true else false
  }

  /**
   * groupbykey是从字段中是截取的
   */
  private def groupKeyOut(topicLogType: String, groupkey: Array[String]) = (record: Array[(String, String)]) => {
    if (groupkey.size == 1 && groupkey(0) == "ad_pos_info") {
      val keyMap = record.toMap
      val ad_pos_info = keyMap("ad_pos_info")
      val groupKey = groupkeyEdit(ad_pos_info)
      (groupKey.mkString, record)
    } else {
      val groupKeySet = Commonhandles.groupkeyEdit(groupkey, topicLogType)
      groupKeySet(record)
    }
  }

  /**
   * edit groupkey
   */
  private def groupkeyEdit(ad_pos_info: String): Array[String] = {

    // 18(slotId_size_广告位ID_landpage_lowprice)
    val ox003: Char = 3
    val ox004: Char = 4
    val bid_ad_info = (ad_pos_info.split(ox003))(0)
    val ad_pos_infoList = bid_ad_info.split(ox004.toString)

    // 广告位ID
    var ad_pos_id = ""
    if (ad_pos_infoList.size > 0) ad_pos_id = ad_pos_infoList(0) else ad_pos_id = ""
    Array(ad_pos_id)
  }
}