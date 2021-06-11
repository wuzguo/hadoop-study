package com.hadoop.study.scala.streaming.table.udf

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/11 14:58
 */

object Table_Udf_AggregateFunction {

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
        // 需要在环境中注册UDF
        tableEnv.createTemporarySystemFunction("funAvg", classOf[AvgTempFunction])

        // call registered function in Table API
        val resultTable = dataTable.groupBy($"id")
          .aggregate(call("funAvg", $"temp").as("tempValue"))
          .select($"id", $"tempValue")
        resultTable.toRetractStream[Row].print("result ")

        // 5.2 SQL
        tableEnv.createTemporaryView("sensors", dataTable)
        val resultSqlTable = tableEnv.sqlQuery("select id, funAvg(temp) as tempValue from sensors group by id");
        resultSqlTable.toRetractStream[Row].print("sql ")

        env.execute("Table UDF AggregateFunction")
    }

    case class TempAccumulator(var sum: Double, var count: Int)

    // 实现自定义AggregateFunction
    class AvgTempFunction extends AggregateFunction[java.lang.Double, TempAccumulator] {

        override def getValue(acc: TempAccumulator): java.lang.Double = acc.sum / acc.count

        override def createAccumulator(): TempAccumulator = TempAccumulator(0.0, 0)

        // 必须实现一个accumulate方法，来数据之后更新状态
        def accumulate(acc: TempAccumulator, temp: java.lang.Double): Unit = {
            acc.sum += temp
            acc.count += 1
        }

        def retract(acc: TempAccumulator, temp: java.lang.Double): Unit = {
            acc.sum -= temp
            acc.count -= 1
        }

        def merge(acc: TempAccumulator, traver: Traversable[TempAccumulator]): Unit = {
            traver.toList.foreach(iter => {
                acc.count += iter.count
                acc.sum += iter.sum
            })
        }

        def resetAccumulator(acc: TempAccumulator): Unit = {
            acc.count = 0
            acc.sum = 0.0
        }
    }
}
