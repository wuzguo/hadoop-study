package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.beans.Rule
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{GCP_PROJECT_NAME, GCP_PUBSUB_RULES_SUBSCRIPTION, RULES_EXPORT_TOPIC, RULES_SINK}
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

object RulesSink extends AbstractSink {

    private val log = LoggerFactory.getLogger("RulesSink")

    override def create(config: Config): SinkFunction[String] = {
        val ruleType = config.get(RULES_SINK)
        SinkType.withName(ruleType.toUpperCase) match {
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
                throw new IllegalArgumentException(s"Source ${sinkType} unknown. Known values are: ${SinkType.values}")
        }
    }

    def streamToJson(alerts: DataStream[Rule]): DataStream[String] =
        alerts.flatMap(JsonSerializer(classOf[Rule])).name("Rules Serialization")
}