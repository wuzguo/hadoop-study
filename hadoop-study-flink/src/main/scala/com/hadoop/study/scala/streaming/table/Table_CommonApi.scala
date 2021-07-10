package com.hadoop.study.scala.streaming.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, TableEnvironment, WithOperations}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 20:05
 */

object Table_CommonApi {

    def main(args: Array[String]): Unit = {
        // 1. 创建环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)

        // 1.1 基于老版本planner的流处理
        val oldStreamSettings = EnvironmentSettings.newInstance.useOldPlanner.inStreamingMode.build
        val oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings)

        // 1.2 基于老版本planner的批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

        // 1.3 基于Blink的流处理
        val blinkStreamSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
        val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

        // 1.4 基于Blink的批处理
        val blinkBatchSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inBatchMode.build
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)


        import org.apache.flink.table.api.DataTypes
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
        val resultTable = sensorTable.select($"id", $"temp").filter($("id").isEqual("sensor_6"))
        // resultTable.toAppendStream[Row].print()

        // 聚合统计
        val aggTable = sensorTable.groupBy($("id")).select($"id", $"id".count.as("count"),
            $"temp".avg.as("avgTemp"))
        // aggTable.toRetractStream[Row].print()

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from sensors where id = 'sensor_6'")
        val sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from sensors group by id")
        val sqlAggStream = sqlAggTable.toRetractStream[Row]
        sqlAggStream.print()

        // 执行
        env.execute("Table CommonApi")
    }
}
