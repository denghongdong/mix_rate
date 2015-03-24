package com.asiainfo.mix.rate

import java.util.Calendar
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.HttpResponse
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject
import scala.collection.mutable.Map
import org.apache.spark.Logging
object HttpUtil extends Serializable{
  
//  val httpMap = XmlAnalysis.getHttpMap
//  val server_type = httpMap("server_type")
//  val oper_type = httpMap("oper_type")
//  val msg_threshold = (httpMap("msg_threshold")).toInt
//  val serverURL = httpMap("serverURL")
//  val contentType = httpMap("contentType")

//  def main(args: Array[String]) {
//
//    val data =
//      """{
//        |"server_type":2,
//        |"oper_type":100,
//        |"data":[
//        |    {"time":1414728188693,"adslot_id":"9223372032560113938","imp_count":2000,"click_count":15,"win_price_sum":2700},
//        |    {"time":1414728188693,"adslot_id":"9223372032561088265","imp_count":2000,"click_count":14,"win_price_sum":2500}
//        | ]
//        |}"""
//  }

  /**
   * sent data by group.
   */
  def msgSend(dataArray:  Array[(String, String)], msgList: Array[String]) = {
    val httpMap = dataArray.toMap
    val msg_threshold = httpMap.getOrElse("msg_threshold", "0")
    val packageList = dataPackageSplit(msg_threshold.toInt, msgList)
    packageList.foreach(packageMsgList => {
      val msg = messageEdit(dataArray, packageMsgList)
      //TODO
//       sendPost(serverURL,contentType)(msg)
      println("send to http response:" + msg)
      println("send:" + msg)
    })
  }

  /**
   * required now time.
   */
  private def getNowtime = (Calendar.getInstance.getTimeInMillis()).toString

  /**
   * mult-message join one message.
   */
  private def messageEdit(dataArray: Array[(String, String)], dataList: ArrayBuffer[String]): String = {
    val httpMap = dataArray.toMap
    val server_type = httpMap("server_type")
    val oper_type = httpMap("oper_type")
    val msg_threshold = (httpMap("msg_threshold")).toInt
    val serverURL = httpMap("serverURL")
    val contentType = httpMap("contentType")

    val head = "{server_type:" + server_type + ",oper_type:" + oper_type + ",data:["
    val tail = "]}"
    head + dataList.mkString("{", "},{", "}") + tail
  }

  /**
   * split data-package.
   * send massages when massage's count is more than msg_threshold create new data-Package.
   */
  private def dataPackageSplit(msg_threshold:Int, msgList: Array[String]): Array[ArrayBuffer[String]] = {
    val nowTime = getNowtime
    val dataPackageList = ArrayBuffer[ArrayBuffer[String]]()
    var dataList: ArrayBuffer[String] = null
    0 until msgList.size map (index => {
      if (index % msg_threshold == 0) {
        dataList = ArrayBuffer[String]()
        dataPackageList += dataList
      }
      dataList += "time:" + nowTime + "," + msgList(index)
    })
    dataPackageList.toArray
  }

    /**
     * set http response
     */
    private def sendPost(serverURL: String, contentType: String) = (msg: String) => {
      val post = new HttpPost(serverURL)
      post.setHeader("Content-Type", contentType)
      val response = new DefaultHttpClient().execute(post)
      post.setEntity(new StringEntity(msg.stripMargin))
      response
    }
}