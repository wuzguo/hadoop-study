package com.hadoop.study.scala.streaming.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 20:05
 */

object Table_FileOutput {

    def main(args: Array[String]): Unit = {
        // 1. 创建环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        val filePath = "./hadoop-study-datas/flink/core/sensor.txt"
        tableEnv.connect(new FileSystem().path(filePath))
          .withFormat(new Csv())
          .withSchema(new Schema()
            .field("id", DataTypes.STRING)
            .field("timestamp", DataTypes.BIGINT)
            .field("temp", DataTypes.DOUBLE))
          .createTemporaryTable("sensors")

        val sensorTable = tableEnv.from("sensors")
        sensorTable.printSchema()
        // sensorTable.toAppendStream[Row].print()

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        val resultTable = sensorTable.select($("id"), $("temp")).filter($("id").isEqual("sensor_6"))
        // resultTable.toAppendStream[Row].print()

        // 输出到文件
        // 输出文件路径
        val output = "./hadoop-study-datas/flink/output/sensor.txt"
        tableEnv.connect(new FileSystem().path(output))
          .withFormat(new Csv)
          .withSchema(new Schema()
            .field("id", DataTypes.STRING)
            // .field("timestamp", DataTypes.BIGINT)
            .field("temp", DataTypes.DOUBLE))
          .createTemporaryTable("outputFile")

        // 执行,输出到表
        resultTable.executeInsert("outputFile")

        // 执行
        // env.execute("Table FileOutput")
    }
}
