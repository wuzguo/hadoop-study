package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{DATA_TOPIC, RECORDS_PER_SECOND, TRANSACTIONS_SOURCE}
import com.hadoop.study.fraud.detect.dynamic.{KafkaUtils, Transaction}
import com.hadoop.study.fraud.detect.functions.{JsonDeserializer, JsonGeneratorWrapper, TimeStamper}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.Logger

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:32
 */

object TransactionsSource {

    def createTransactionsSource(config: Config): SourceFunction[String] = {
        val sourceType = config.get(TRANSACTIONS_SOURCE)

        val transactionsSourceType = SourceType.withName(sourceType.toUpperCase)
        if (transactionsSourceType eq SourceType.KAFKA) {
            val kafkaProps = KafkaUtils.initConsumerProperties(config)
            val transactionsTopic = config.get(DATA_TOPIC)
            val kafkaConsumer = new FlinkKafkaConsumer[String](transactionsTopic, new SimpleStringSchema, kafkaProps)
            kafkaConsumer.setStartFromLatest()
            kafkaConsumer
        }
        else {
            val transactionsPerSecond = config.get(RECORDS_PER_SECOND)
            JsonGeneratorWrapper(TransactionsGenerator(transactionsPerSecond))
        }
    }

    def stringsStreamToTransactions(stringStream: DataStream[String], log: Logger): DataStream[Transaction] =
        stringStream
          .flatMap(JsonDeserializer(classOf[Transaction], log))
          .returns(classOf[Transaction])
          .flatMap(new TimeStamper)
          .returns(classOf[Transaction])
          .name("Transactions Deserialization")
}


object SourceType extends Enumeration {
    type SourceType = Value

    val GENERATOR, KAFKA = Value
}