package com.asiainfo.mix.rate

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

/**
 * @author surq
 * @since 2014.10.28
 * 功能解介：<br>
 * 根据配置文件定义的维度，统计出达到一个阀值时的各累计字段的值。<br>
 */
object MixAateApp extends Logging {

  def main(args: Array[String]): Unit = {

    XmlAnalysis.annalysis("conf/logconf.xml")
    val AppPropertiesMap = XmlAnalysis.getAppPropertiesMap
    val logStructMap = XmlAnalysis.getLogStructMap

    val appName = AppPropertiesMap("appName")
    val streamSpace = AppPropertiesMap("interval")
    val output_prefix = AppPropertiesMap("output_prefix")
    val output_suffix = AppPropertiesMap("output_suffix")
    val expose_threshold = AppPropertiesMap("expose_threshold")
    val checkpointPath = AppPropertiesMap("checkpointPath")
    val setThreshold = Commonhandles.updateFunc(expose_threshold.toInt)

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
      val itemszip = Commonhandles.zipKeyValue(items)
      val stream = ssc.textFileStream(hdfsPath).filter(Commonhandles.valadityCheck).map(itemszip)
      //      Commonhandles.compute(stream, propMap)
      val clz = Class.forName(appClass)
      val constructors = clz.getConstructors()
      val constructor = constructors(0).newInstance()
      val outputStream = constructor.asInstanceOf[StreamAction].run(stream, propMap)
      outputStream
    })

    // join handle 二选一
//         val joinsDstreams = dstreamList.reduceLeft(Commonhandles.dstreamsJoin).updateStateByKey(setThreshold).map(Commonhandles.dataMergeBykey)

    //     union handle
    val joinsDstreams = dstreamList.reduceLeft(Commonhandles.dstreamsUnion).updateStateByKey(setThreshold).map(Commonhandles.dataMergeBykey)
    joinsDstreams.saveAsTextFiles(output_prefix, output_suffix)
    ssc.start()
    ssc.awaitTermination()
  }
}