package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.beans.{AlertEvent, Transaction}
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{ALERTS_SINK, ALERTS_TOPIC, GCP_PROJECT_NAME, GCP_PUBSUB_ALERTS_SUBSCRIPTION}
import com.hadoop.study.fraud.detect.dynamic.KafkaUtils
import com.hadoop.study.fraud.detect.functions.JsonSerializer
import com.hadoop.study.fraud.detect.sinks.AlertsType.{DISCARD, KAFKA, PUBSUB, STDOUT}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.DataStream
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

object AlertsSink {

    private val log = LoggerFactory.getLogger("AlertsSink")

    def createAlertsSink(config: Config): SinkFunction[String] = {
        val sinkType = config.get(ALERTS_SINK)
        val alertsSinkType = AlertsType.withName(sinkType.toUpperCase)
        alertsSinkType match {
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
                throw new IllegalArgumentException(s"Source ${alertsSinkType} unknown. Known values are: ${AlertsType.values}")
        }
    }

    def alertsStreamToJson(alerts: DataStream[AlertEvent[Transaction, BigDecimal]]): DataStream[String] =
        alerts.flatMap(JsonSerializer(classOf[AlertEvent[Transaction, BigDecimal]], log))
          .name("Alerts Serialization")
}

object AlertsType extends Enumeration {
    type AlertsType = Value

    val KAFKA, PUBSUB, STDOUT, DISCARD = Value
}