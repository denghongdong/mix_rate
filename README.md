1、程序的入口函数类：
com.asiainfo.mix.rate.MixAateApp
本机调试运行：
直接运行 com.asiainfo.mix.rate.MixAateApp即可。
spark yarn运行：
./bin/spark-submit --class com.asiainfo.mix.rate.MixAateApp ./lib/streaming-log-0.0.1-SNAPSHOT-jar-with-dependencies.jar

2、关于配置文件参数说明：
<configuration>
	<appProperties>
		<appName>系统app名称</appName>
		<interval>spark Streaming 生成job时间间隔</interval>
		<output_prefix>spark输出文件夹路径前缀</output_prefix>
		<output_suffix>spark输出文件夹路径前缀</output_suffix>
		<separator>mix日志文件内容分隔符</separator>
		<expose_threshold>曝光达到的上限阀值</expose_threshold>
		<checkpointPath>Streaming HDFS的 checkpoint路径</checkpointPath>
	</appProperties>
	<logProperties>	
		<log>
			<topicLogType>日志类型</topicLogType>
			<appClass>本类型日志的个性化处理</appClass>
			<hdfsPath>存储日志HDFS路径</hdfsPath>
			<items>日志结构定义</items>
			<groupByKey>统计维度关键字</groupByKey>
			<outputItems>统计关键字，若统计条数请用"[count]"</outputItems>
		</log>
		<log>...</log>
		<log>...</log>
	</logProperties>
</configuration>