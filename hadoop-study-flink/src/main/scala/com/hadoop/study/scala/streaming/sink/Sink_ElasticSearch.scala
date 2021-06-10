package com.hadoop.study.scala.streaming.sink

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 10:07
 */

object Sink_ElasticSearch {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 构造Stream
        val sensorStream = fileStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        val httpHosts = new util.ArrayList[HttpHost]
        httpHosts.add(new HttpHost("hadoop001", 9200))

        val esSink = new ElasticsearchSink.Builder[Sensor](httpHosts, new CustomSinkFunction).build()
        sensorStream.addSink(esSink)

        // 执行
        env.execute("Streaming Sink ElasticSearch")
    }

    // 定义 Sink
    class CustomSinkFunction extends ElasticsearchSinkFunction[Sensor] {
        override def process(element: Sensor, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
            // 定义写入的数据source
            val dataSource = Map("id" -> element.id, "temp" -> element.temp.toString, "timestamp" -> element.timestamp.toString)
            // 创建请求，作为向es发起的写入命令
            val request = Requests.indexRequest().index("sink-sensor").source(dataSource)
            // 用index发送请求
            indexer.add(request)
        }
    }
}
