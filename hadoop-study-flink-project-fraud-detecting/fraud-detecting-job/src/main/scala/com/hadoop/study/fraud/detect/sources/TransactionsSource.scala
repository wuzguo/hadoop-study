package com.hadoop.study.fraud.detect.sources

import com.hadoop.study.fraud.detect.beans.Transaction
import com.hadoop.study.fraud.detect.config.Config
import com.hadoop.study.fraud.detect.config.Parameters.{DATA_TOPIC, RECORDS_PER_SECOND, TRANSACTIONS_SOURCE}
import com.hadoop.study.fraud.detect.enums.SourceType
import com.hadoop.study.fraud.detect.enums.TransactionType.KAFKA
import com.hadoop.study.fraud.detect.functions.{JsonDeserializer, JsonGeneratorWrapper, TimeStamper}
import com.hadoop.study.fraud.detect.utils.KafkaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:32
 */

object TransactionsSource extends AbstractSource {

    private val log = LoggerFactory.getLogger("TransactionsSource")

    override def getSourceType(config: Config): SourceType.Value = {
        val sourceType = config.get(TRANSACTIONS_SOURCE)
        SourceType.withName(sourceType.toUpperCase)
    }

    override def create(config: Config): SourceFunction[String] = {
        log.info(s"RulesSource config: ${config}")
        getSourceType(config) match {
            case KAFKA =>
                val kafkaProps = KafkaUtils.initConsumerProperties(config)
                val transactionsTopic = config.get(DATA_TOPIC)
                val kafkaConsumer = new FlinkKafkaConsumer[String](transactionsTopic, new SimpleStringSchema, kafkaProps)
                kafkaConsumer.setStartFromLatest()
                kafkaConsumer
            case _ =>
                val transactionsPerSecond = config.get(RECORDS_PER_SECOND)
                JsonGeneratorWrapper(TransactionsGenerator(transactionsPerSecond))
        }
    }

    def streamToTransactions(stringStream: DataStream[String]): DataStream[Transaction] =
        stringStream.flatMap(JsonDeserializer(classOf[Transaction]))
          .map(new TimeStamper[Transaction])
          .name("Transactions Deserialization")
}