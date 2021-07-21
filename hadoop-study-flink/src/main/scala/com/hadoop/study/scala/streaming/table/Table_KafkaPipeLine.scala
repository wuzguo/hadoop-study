package com.hadoop.study.scala.streaming.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, FieldExpression}
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.kafka.clients.producer.ProducerConfig

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 20:05
 */

object Table_KafkaPipeLine {

    def main(args: Array[String]): Unit = {
        // 1. 创建环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 从Kafka读取数据
        tableEnv.connect(new Kafka()
          .version("universal")
          .topic("topic_streaming")
          .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop002:9092")
          .property(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          .property(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"))
          .withFormat(new Csv().fieldDelimiter(' '))
          .withSchema(new Schema()
            .field("timestamp", DataTypes.BIGINT)
            .field("area", DataTypes.STRING)
            .field("city", DataTypes.STRING)
            .field("userId", DataTypes.INT)
            .field("adId", DataTypes.INT))
          .createTemporaryTable("userInfos")

        val userInfos = tableEnv.from("userInfos")
        userInfos.printSchema()
        // userInfos.execute().print()

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        val users = userInfos
          .select($"timestamp", $"area", $"city", $"userId", $"adId")
          .filter($("userId").isNotNull)
        // users.toAppendStream[Row].print()

        // 输出到Kafka
        // 输出文件路径
        tableEnv.connect(new Kafka()
          .version("universal")
          .topic("topic_streaming_out")
          .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop002:9092")
          .property(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          .property(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"))
          .withFormat(new Csv())
          .withSchema(new Schema()
            .field("timestamp", DataTypes.BIGINT)
            .field("area", DataTypes.STRING)
            .field("city", DataTypes.STRING)
            .field("userId", DataTypes.INT)
            .field("adId", DataTypes.INT))
          .createTemporaryTable("outUsers")
        // 执行,输出到表
        users.executeInsert("outUsers")

        // 执行
        //  env.execute("Table FileOutput")
    }
}
