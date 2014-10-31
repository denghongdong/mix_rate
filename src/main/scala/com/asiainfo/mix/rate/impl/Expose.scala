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
 * 曝光日志处理逻辑<br>
 */
class Expose extends StreamAction with Serializable{
  override def run(inputStream: DStream[Array[(String, String)]], propMap: Map[String, String]): DStream[(String, String)] = Commonhandles.compute(inputStream, propMap)
}