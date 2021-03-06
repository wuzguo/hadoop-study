package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.beans.{AlertEvent, Transaction}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{ALERTS_SINK, ALERTS_TOPIC, GCP_PROJECT_NAME, GCP_PUBSUB_ALERTS_SUBSCRIPTION}
import com.hadoop.study.fraud.detect.enums.SinkType
import com.hadoop.study.fraud.detect.enums.SinkType.{DISCARD, KAFKA, PUBSUB, STDOUT}
import com.hadoop.study.fraud.detect.functions.JsonSerializer
import com.hadoop.study.fraud.detect.utils.KafkaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.LoggerFactory

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:56
 */

object AlertsSink extends AbstractSink {

    private val log = LoggerFactory.getLogger("AlertsSink")

    override def create(config: Config): SinkFunction[String] = {
        val alertType = config.get(ALERTS_SINK)
        SinkType.withName(alertType.toUpperCase) match {
            case KAFKA =>
                val kafkaProps = KafkaUtils.initProducerProperties(config)
                val alertsTopic = config.get(ALERTS_TOPIC)
                new FlinkKafkaProducer(alertsTopic, new SimpleStringSchema, kafkaProps)
            case PUBSUB =>
                PubSubSink.newBuilder()
                  .withSerializationSchema(new SimpleStringSchema)
                  .withProjectName(config.get(GCP_PROJECT_NAME))
                  .withTopicName(config.get(GCP_PUBSUB_ALERTS_SUBSCRIPTION))
                  .build
            case STDOUT =>
                new PrintSinkFunction[String](true)
            case DISCARD =>
                new DiscardingSink[String]
            case _ =>
                throw new IllegalArgumentException(s"source ${alertType} unknown. Known values are: ${SinkType.values}")
        }
    }

    def streamToJson(alerts: DataStream[AlertEvent[Transaction, BigDecimal]]): DataStream[String] =
        alerts.flatMap(JsonSerializer(classOf[AlertEvent[Transaction, BigDecimal]]))
          .name("Alerts Serialization")
}