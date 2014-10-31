package com.asiainfo.mix.rate

import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2014.10.30
 * 所有log处理都要实现此类<br>
 */
abstract class StreamAction {
  
 def run(inputStream:DStream[Array[(String, String)]],propMap:Map[String,String]): DStream[(String,String)]
}