package com.hadoop.study.fraud.detect.sinks

import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{GCP_PROJECT_NAME, GCP_PUBSUB_LATENCY_SUBSCRIPTION, LATENCY_SINK, LATENCY_TOPIC}
import com.hadoop.study.fraud.detect.dynamic.KafkaUtils
import com.hadoop.study.fraud.detect.sinks.LatencyType._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.io.IOException

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 17:11
 */

object LatencySink {

    @throws[IOException]
    def createLatencySink(config: Config): SinkFunction[String] = {
        val latencySink = config.get(LATENCY_SINK)
        val latencySinkType = LatencyType.withName(latencySink.toUpperCase)
        latencySinkType match {
            case KAFKA =>
                val kafkaProps = KafkaUtils.initProducerProperties(config)
                val latencyTopic = config.get(LATENCY_TOPIC)
                new FlinkKafkaProducer(latencyTopic, new SimpleStringSchema, kafkaProps)
            case PUBSUB =>
                PubSubSink.newBuilder()
                  .withSerializationSchema(new SimpleStringSchema)
                  .withProjectName(config.get(GCP_PROJECT_NAME))
                  .withTopicName(config.get(GCP_PUBSUB_LATENCY_SUBSCRIPTION))
                  .build
            case STDOUT =>
                new PrintSinkFunction[String](true)
            case DISCARD =>
                new DiscardingSink[String]
            case _ =>
                throw new IllegalArgumentException(s"Source ${latencySinkType} unknown. Known values are: ${LatencyType.values}")
        }
    }
}

object LatencyType extends Enumeration {
    type Type = Value

    val KAFKA, PUBSUB, STDOUT, DISCARD = Value
}
