package com.asiainfo.mix.rate

import scala.xml.XML
import scala.collection.mutable.Map
import scala.beans.BeanProperty

/**
 * @author surq
 * @since 2014.10.28
 * 功能解介：<br>
 * 配置文件解析<br>
 */
object XmlAnalysis {

  // log 日志属性配置
  @BeanProperty val appPropertiesMap = Map[String, String]()
  @BeanProperty val hdfsScannerMap = Map[String, String]()
  @BeanProperty val httpMap = Map[String, String]()
  @BeanProperty val logStructMap = Map[String, Map[String, String]]()

  def annalysis(propertyFilePath: String) = {

    val xmlFile = XML.load(propertyFilePath)

    //-----------------------appProperties --------------------
    val appNode = xmlFile \ "appProperties"
    val appName = (appNode \ "appName").text.toString.trim()
    val interval = (appNode \ "interval").text.toString.trim()
    val output_prefix = (appNode \ "output_prefix").text.toString.trim()
    val output_suffix = (appNode \ "output_suffix").text.toString.trim()
    var separator = (appNode \ "separator").text.toString
    val expose_threshold = (appNode \ "expose_threshold").text.toString
    val checkpointPath = (appNode \ "checkpointPath").text.toString
    val msgStruct = (appNode \ "msgStruct").text.toString
    val msgItems = (appNode \ "msgItems").text.toString

    appPropertiesMap += ("appName" -> appName)
    appPropertiesMap += ("interval" -> interval)
    appPropertiesMap += ("output_prefix" -> output_prefix)
    appPropertiesMap += ("output_suffix" -> output_suffix)
    val ox002: Char = 2
    if (separator == "") separator = ox002.toString
    appPropertiesMap += ("separator" -> separator)
    appPropertiesMap += ("expose_threshold" -> expose_threshold)
    appPropertiesMap += ("checkpointPath" -> checkpointPath)
    appPropertiesMap += ("msgStruct" -> msgStruct)
    appPropertiesMap += ("msgItems" -> msgItems)

    //----------------- HDFS scanner ------------------------    
    val scanner = xmlFile \ "hdfsScanner"
    val scanInterval = (scanner \ "scanInterval").text.toString
    val hdfsStartTime = (scanner \ "hdfsStartTime").text.toString
    val localLogDir = (scanner \ "localLogDir").text.toString
    hdfsScannerMap += ("scanInterval" -> scanInterval)
    hdfsScannerMap += ("hdfsStartTime" -> hdfsStartTime)
    hdfsScannerMap += ("localLogDir" -> localLogDir)

    //-------------------http post ------------------------
    val sendMode = xmlFile \ "sendMode"
    val serverURL = (sendMode \ "serverURL").text.toString.trim()
    val contentType = (sendMode \ "Content_Type").text.toString.trim()
    val server_type = (sendMode \ "server_type").text.toString.trim()
    val oper_type = (sendMode \ "oper_type").text.toString.trim()
    val msg_threshold = (sendMode \ "msg_threshold").text.toString.trim()

    httpMap += ("serverURL" -> serverURL)
    httpMap += ("contentType" -> contentType)
    httpMap += ("server_type" -> server_type)
    httpMap += ("oper_type" -> oper_type)
    httpMap += ("msg_threshold" -> msg_threshold)
    //-------------------logProperties ------------------------
    val mixlogs = xmlFile \ "logProperties" \ "log"
    mixlogs.map(p => {
      val mixLogMap = Map[String, String]()
      val topicLogType = (p \ "topicLogType").text.toString.trim()
      val appClass = (p \ "appClass").text.toString.trim()
      val sourceHDFSPath = (p \ "sourceHDFSPath").text.toString.trim()
      val hdfsPath = (p \ "hdfsPath").text.toString.trim()
      val items = (p \ "items").text.toString.trim()
      val groupByKey = (p \ "groupByKey").text.toString.trim()
      val outputItems = (p \ "outputItems").text.toString.trim()

      mixLogMap += ("topicLogType" -> topicLogType)
      mixLogMap += ("appClass" -> appClass)
      mixLogMap += ("sourceHDFSPath" -> sourceHDFSPath)
      mixLogMap += ("hdfsPath" -> hdfsPath)
      mixLogMap += ("items" -> items)
      mixLogMap += ("groupByKey" -> groupByKey)
      mixLogMap += ("outputItems" -> outputItems)

      logStructMap += (topicLogType -> mixLogMap)
    })
  }
}