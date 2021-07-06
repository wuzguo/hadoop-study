package com.hadoop.study.flink.analysis

import com.hadoop.study.flink.analysis.beans.UserBehavior
import com.hadoop.study.flink.analysis.utils.WebSocketClient
import org.apache.flink.api.common.eventtime.{TimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.java_websocket.client.WebSocketClient

import java.sql.Timestamp
import java.time.Duration
import java.util
import java.util.Properties

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/21 19:15
 */

object PageViewAnalysisWithSink {

    def main(args: Array[String]): Unit = {
        val parameterTool = ParameterTool.fromArgs(args)
        val kafkaTopic = parameterTool.get("topic", "topic_behavior")
        val brokers = parameterTool.get("broker", "10.20.0.92:9092")

        println(s"reading from kafka topic ${kafkaTopic} @ ${brokers}")

        val properties = new Properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-behavior-analysis")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

        // 读取数据
        val kafkaConsumer = new FlinkKafkaConsumer[UserBehavior](kafkaTopic, new UserBehaviorSchema, properties)
        kafkaConsumer.setStartFromGroupOffsets()
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false)

        kafkaConsumer.assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(_ => new PeriodicWatermarkGenerator(Duration.ofSeconds(0)))
              .withTimestampAssigner(_ => new TimeStampExtractor()))

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 读取数据
        val inputStream = env.addSource(kafkaConsumer)

        // 开窗，计算
        val dataStream = inputStream.filter(_.action == "pv")
          .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
          .allowedLateness(Time.seconds(30))
          .process(new BehaviorWindowFunction)

        dataStream.print("统计结果： ").setParallelism(1)

        // 增加 ES Sink
        val httpHosts = new util.ArrayList[HttpHost]
        httpHosts.add(new HttpHost("hadoop001", 9200))

        // 获取Index
        val index = parameterTool.get("index", "index-behavior-analysis")
        val esSink = new ElasticsearchSink.Builder[(Long, Long, Long, Integer)](httpHosts, new CustomSinkFunction(index)).build()
        dataStream.addSink(esSink).setParallelism(1)

        // 增加 WebSocket Sink
        dataStream.addSink(new WebsocketSink("ws://localhost:18008/ws")).setParallelism(1)

        // 执行
        env.execute(parameterTool.get("appName", "Page View Analysis With Sink"))
    }

    class PeriodicWatermarkGenerator(maxOutOfOrderness: Duration) extends WatermarkGenerator[UserBehavior] {

        private val outOfOrdernessMillis = maxOutOfOrderness.toMillis

        private var currentWatermark = Long.MinValue + outOfOrdernessMillis + 1

        override def onEvent(event: UserBehavior, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            currentWatermark = Math.max(currentWatermark, eventTimestamp)
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(currentWatermark - outOfOrdernessMillis - 1))
        }
    }

    class TimeStampExtractor extends TimestampAssigner[UserBehavior] {
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = element.timestamp.getTime
    }

    class UserBehaviorSchema extends DeserializationSchema[UserBehavior] with SerializationSchema[UserBehavior] {
        override def deserialize(message: Array[Byte]): UserBehavior = {
            val behavior = new String(message)
            val values = behavior.split(",")
            UserBehavior(values(0).toLong, values(1).toLong, values(2).toInt, values(3), new Timestamp(values(4).toLong))
        }

        override def isEndOfStream(nextElement: UserBehavior): Boolean = {
            false
        }

        override def serialize(element: UserBehavior): Array[Byte] = {
            element.toString.getBytes
        }

        override def getProducedType: TypeInformation[UserBehavior] = {
            TypeInformation.of(classOf[UserBehavior])
        }
    }

    // 结果函数
    class BehaviorWindowFunction extends ProcessAllWindowFunction[UserBehavior, (Long, Long, Long, Integer), TimeWindow] {
        override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[(Long, Long, Long, Integer)]): Unit = {
            var viewCount = 0
            var userIds = Set[Long]()
            val iterator = elements.iterator
            while (iterator.hasNext) {
                val behavior = iterator.next()
                viewCount += 1
                userIds += behavior.userId
            }

            val window = context.window
            out.collect((window.getStart, window.getEnd, viewCount, userIds.size))
        }
    }

    class CustomFailureHandler(index: String) extends ActionRequestFailureHandler {
        override def onFailure(actionRequest: ActionRequest, throwable: Throwable, i: Int, requestIndexer: RequestIndexer): Unit = {
            actionRequest match {
                case request: IndexRequest =>
                    val mapDatas = Map("data" -> request.source())
                    requestIndexer.add(Requests.indexRequest.index(index).id(request.id).source(mapDatas))

                case _ => throw new IllegalStateException("unexpected")
            }
        }
    }

    // 定义 Sink
    class CustomSinkFunction(index: String) extends ElasticsearchSinkFunction[(Long, Long, Long, Integer)] {
        override def process(element: (Long, Long, Long, Integer), ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
            // 定义写入的数据source
            val dataSource = Map("window_start" -> element._1, "window_end" -> element._2, "pv" -> element._3, "uv" -> element._4)
            // 创建请求，作为向es发起的写入命令
            val request = Requests.indexRequest().index(index).source(dataSource)
            // 用index发送请求
            indexer.add(request)
        }
    }

    // 定义WebSocket Sink
    class WebsocketSink(url: String) extends RichSinkFunction[(Long, Long, Long, Integer)] {

        private var wsClient: WebSocketClient = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            wsClient = WebSocketClient(url)
            wsClient.init()
        }

        override def invoke(value: (Long, Long, Long, Integer), context: SinkFunction.Context): Unit = {
            val message = s"window_start: ${value._1},window_end:${value._2},pv:${value._3},uv:${value._4}"
            wsClient.send(message)
        }
    }
}
