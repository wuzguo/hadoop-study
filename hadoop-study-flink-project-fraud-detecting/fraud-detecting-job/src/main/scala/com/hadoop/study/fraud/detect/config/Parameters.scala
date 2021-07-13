package com.hadoop.study.fraud.detect.config

import org.apache.flink.api.java.utils.ParameterTool

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 9:38
 */

case class Parameters(tool: ParameterTool) {

    def getOrDefault[T](param: Param[T]): T = {
        // 如果没有值直接返回
        if (!tool.has(param.name)) return param.default

        val value =
            if (param.cls eq classOf[Integer]) tool.getInt(param.name)
            else if (param.cls eq classOf[Long]) tool.getLong(param.name)
            else if (param.cls eq classOf[Double]) tool.getDouble(param.name)
            else if (param.cls eq classOf[Boolean]) tool.getBoolean(param.name)
            else tool.get(param.name)

        param.cls.cast(value)
    }
}

object Parameters {

    // Kafka:
    val KAFKA_HOST: Param[String] = Param.string("kafka-host", "10.20.0.92")
    val KAFKA_PORT: Param[Integer] = Param.integer("kafka-port", 9092)
    val DATA_TOPIC: Param[String] = Param.string("data-topic", "topic-detect-live-transactions")
    val ALERTS_TOPIC: Param[String] = Param.string("alerts-topic", "topic-detect-alerts")
    val RULES_TOPIC: Param[String] = Param.string("rules-topic", "topic-detect-rules")
    val LATENCY_TOPIC: Param[String] = Param.string("latency-topic", "topic-detect-latency")
    val RULES_EXPORT_TOPIC: Param[String] = Param.string("current-rules-topic", "topic-detect-current-rules")
    val OFFSET: Param[String] = Param.string("offset", "latest")

    // GCP PubSub:
    val GCP_PROJECT_NAME: Param[String] = Param.string("gcp-project", "da-fe-212612")
    val GCP_PUBSUB_RULES_SUBSCRIPTION: Param[String] = Param.string("pubsub-rules", "rules-demo")
    val GCP_PUBSUB_ALERTS_SUBSCRIPTION: Param[String] = Param.string("pubsub-alerts", "alerts-demo")
    val GCP_PUBSUB_LATENCY_SUBSCRIPTION: Param[String] = Param.string("pubsub-latency", "latency-demo")
    val GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION: Param[String] = Param.string("pubsub-rules-export", "current-rules-demo")

    // Socket
    val SOCKET_PORT: Param[Integer] = Param.integer("pubsub-rules-export", 6800)

    // General:
    // source/sink types: kafka / pubsub / socket
    val RULES_SOURCE: Param[String] = Param.string("rules-source", "SOCKET")
    val TRANSACTIONS_SOURCE: Param[String] = Param.string("data-source", "GENERATOR")
    val ALERTS_SINK: Param[String] = Param.string("alerts-sink", "STDOUT")
    val LATENCY_SINK: Param[String] = Param.string("latency-sink", "STDOUT")
    val RULES_SINK: Param[String] = Param.string("rules-sink", "STDOUT")
    val RECORDS_PER_SECOND: Param[Integer] = Param.integer("records-per-second", 2)
    val LOCAL_MODE_DISABLE_WEB_UI = "-1"
    val LOCAL_EXECUTION: Param[String] = Param.string("local", LOCAL_MODE_DISABLE_WEB_UI)
    val SOURCE_PARALLELISM: Param[Integer] = Param.integer("source-parallelism", 1)
    val SINK_PARALLELISM: Param[Integer] = Param.integer("sink-parallelism", 1)
    val CHECKPOINT_INTERVAL: Param[Integer] = Param.integer("checkpoint-interval", 60000)
    val CHECKPOINT_TIMEOUT: Param[Integer] = Param.integer("checkpoint-timeout", 60000)
    val MIN_PAUSE_BETWEEN_CHECKPOINTS: Param[Integer] = Param.integer("min-pause-between-checkpoints", 30000)
    val OUT_OF_ORDERLESS: Param[Integer] = Param.integer("out-of-orderliness", 500)

    val STRING_PARAMS = List(LOCAL_EXECUTION, KAFKA_HOST, DATA_TOPIC, ALERTS_TOPIC, RULES_TOPIC, LATENCY_TOPIC, RULES_EXPORT_TOPIC, OFFSET, GCP_PROJECT_NAME, GCP_PUBSUB_RULES_SUBSCRIPTION, GCP_PUBSUB_ALERTS_SUBSCRIPTION, GCP_PUBSUB_LATENCY_SUBSCRIPTION, GCP_PUBSUB_RULES_EXPORT_SUBSCRIPTION, RULES_SOURCE, TRANSACTIONS_SOURCE, ALERTS_SINK, LATENCY_SINK, RULES_SINK)

    val INT_PARAMS = List(KAFKA_PORT, SOCKET_PORT, RECORDS_PER_SECOND, SOURCE_PARALLELISM, SINK_PARALLELISM, CHECKPOINT_INTERVAL, CHECKPOINT_TIMEOUT, MIN_PAUSE_BETWEEN_CHECKPOINTS, OUT_OF_ORDERLESS)

    val BOOL_PARAMS: List[Param[Boolean]] = List[Param[Boolean]]()

    def fromArgs(args: Array[String]): Parameters = {
        val tool = ParameterTool.fromArgs(args)
        new Parameters(tool)
    }
}