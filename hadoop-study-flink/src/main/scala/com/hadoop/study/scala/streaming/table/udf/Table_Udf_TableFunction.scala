package com.hadoop.study.scala.streaming.table.udf

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/11 14:58
 */

object Table_Udf_TableFunction {

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
        val dataTable = tableEnv.fromDataStream(sensorStream, $"id", $"timestamp", $"temp")

        // 5. 自定义标量函数，实现求id的hash值
        // 5.1 table API
        val splitter = new SplitFunction("_")
        // 需要在环境中注册UDF
        tableEnv.createTemporarySystemFunction("split", splitter)

        // call registered function in Table API
        val resultTable = dataTable.joinLateral(call("split", $"id")).select($"id", $"timestamp", $"word", $"length")
        resultTable.toAppendStream[Row].print("result ")

        // 5.2 SQL
        tableEnv.createTemporaryView("sensors", dataTable)
        val resultSqlTable = tableEnv.sqlQuery("select id, word, length from sensors, lateral table(split(id)) as splitId(word, length)")
        resultSqlTable.toAppendStream[Row].print("sql ")

        env.execute("Table UDF TableFunction")
    }

    // 实现自定义TableFunction
    @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
    class SplitFunction(separator: String) extends TableFunction[Row] {

        // 必须实现一个eval方法，没有返回值
        def eval(value: String): Unit = {
            for (str <- value.split(separator)) {
                collect(Row.of(str, Int.box(str.length)))
            }
        }
    }
}
