package com.hadoop.study.scala.streaming.table

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 17:02
 */

object Table_Example {

    def main(args: Array[String]): Unit = {
        // 0. 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 1. 读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 2. 转换成POJO
        // 构造Stream
        val sensorStream = fileStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        // 3. 创建表环境
        val tableEnv = StreamTableEnvironment.create(env)

        // 4. 基于流创建一张表
        val dataTable = tableEnv.fromDataStream(sensorStream)

        // 5. 调用table API进行转换操作 DSL
        val resultTable = dataTable.select($"id", $"temp").where($("id").isEqual("sensor_1"))
        val resultStream = resultTable.toAppendStream[Row]
        resultStream.print("result ")

        // 6. 创建视图，执行SQL
        tableEnv.createTemporaryView("sensor", dataTable)
        val sql = "select * from sensor where id = 'sensor_1'"
        val resultSqlTable = tableEnv.sqlQuery(sql)
        val resultSqlStream = resultSqlTable.toAppendStream[Row]
        resultSqlStream.print("sql ")

        // 7. 执行
        env.execute("Table Example")
    }
}
