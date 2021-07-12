package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{GCP_PROJECT_NAME, GCP_PUBSUB_RULES_SUBSCRIPTION, RULES_EXPORT_SINK, RULES_EXPORT_TOPIC}
import com.hadoop.study.fraud.detect.enums.SinkType
import com.hadoop.study.fraud.detect.enums.SinkType.{KAFKA, PUBSUB, STDOUT}
import com.hadoop.study.fraud.detect.functions.JsonSerializer
import com.hadoop.study.fraud.detect.utils.KafkaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.LoggerFactory

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 17:03
 */

object CurrentRulesSink {

    private val log = LoggerFactory.getLogger("CurrentRulesSink")

    def createRulesSink(config: Config): SinkFunction[String] = {
        val sinkType = config.get(RULES_EXPORT_SINK)
        val currentRulesSinkType = SinkType.withName(sinkType.toUpperCase)
        currentRulesSinkType match {
            case KAFKA =>
                val kafkaProps = KafkaUtils.initProducerProperties(config)
                val alertsTopic = config.get(RULES_EXPORT_TOPIC)
                new FlinkKafkaProducer(alertsTopic, new SimpleStringSchema, kafkaProps)
            case PUBSUB =>
                PubSubSink.newBuilder()
                  .withSerializationSchema(new SimpleStringSchema)
                  .withProjectName(config.get(GCP_PROJECT_NAME))
                  .withTopicName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
                  .build
            case STDOUT =>
                new PrintSinkFunction[String](true)
            case _ =>
                throw new IllegalArgumentException(s"Source ${currentRulesSinkType} unknown. Known values are: ${SinkType.values}")
        }
    }

    def rulesStreamToJson(alerts: DataStream[Rule]): DataStream[String] =
        alerts.flatMap(JsonSerializer(classOf[Rule])).name("Rules Serialization")
}