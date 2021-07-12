package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters._
import com.hadoop.study.fraud.detect.enums.SourceType
import com.hadoop.study.fraud.detect.enums.SourceType.{KAFKA, PUBSUB, SOCKET, STATIC}
import com.hadoop.study.fraud.detect.functions.RuleDeserializer
import com.hadoop.study.fraud.detect.utils.KafkaUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{SocketTextStreamFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.time.Duration

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 14:28
 */

object RulesSource {

    def create(config: Config): SourceFunction[String] = {
        val sourceType: String = config.get(RULES_SOURCE)
        val rulesSourceType = SourceType.withName(sourceType.toUpperCase)

        rulesSourceType match {
            case KAFKA =>
                val kafkaProps = KafkaUtils.initConsumerProperties(config)
                val rulesTopic = config.get(RULES_TOPIC)
                val kafkaConsumer = new FlinkKafkaConsumer[String](rulesTopic, new SimpleStringSchema, kafkaProps)
                kafkaConsumer.setStartFromLatest()
                kafkaConsumer
            case PUBSUB =>
                PubSubSource.newBuilder()
                  .withDeserializationSchema(new SimpleStringSchema)
                  .withProjectName(config.get(GCP_PROJECT_NAME))
                  .withSubscriptionName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
                  .build
            case SOCKET =>
                new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1)
            case STATIC =>
                RulesStaticJsonGenerator()
            case _ =>
                throw new IllegalArgumentException(s"Source ${rulesSourceType} unknown. Known values are: ${SourceType.values}")
        }
    }

    def streamToRules(ruleStream: DataStream[String]): DataStream[Rule] =
        ruleStream.flatMap(RuleDeserializer())
          .name("Rule Deserialization")
          .setParallelism(1)
          .assignTimestampsAndWatermarks(
              WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner[Rule] {
                    override def extractTimestamp(element: Rule, recordTimestamp: Long): Long = Long.MaxValue
                }))
}


