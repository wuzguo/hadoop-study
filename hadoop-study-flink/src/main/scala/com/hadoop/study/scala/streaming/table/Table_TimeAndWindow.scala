package com.hadoop.study.scala.streaming.table

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api.{FieldExpression, LiteralIntExpression, Tumble, WithOperations}
import org.apache.flink.types.Row

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/11 11:46
 */

object Table_TimeAndWindow {

    def main(args: Array[String]): Unit = {
        // 1. 获取环境配置
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val tableEnv = StreamTableEnvironment.create(env)
        // 2. 从Socket读取文件
        val dss: DataStream[String] = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 3. 转换成Sensor类型，分配时间戳和watermark
        val dataStream: DataStream[Sensor] = dss.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        }).assignTimestampsAndWatermarks(
            new AssignerWithPeriodicWatermarksAdapter.Strategy(new TimestampExtractor(Time.seconds(1))))

        // 4. 将流转换成表，定义时间特性
        val dataTable = tableEnv.fromDataStream(dataStream, $"id", $"timestamp", $"temp", $"rt".rowtime)
        tableEnv.createTemporaryView("sensors", dataTable)

        // 5. 窗口操作
        // 5.1 Group Window
        // table API

        // 其他写法： val resultTable = dataTable.window(Tumble over 10.second on $"rt" as "tw")
        val resultTable = dataTable.window(Tumble.over(10.seconds).on($"rt").as("tw"))
          .groupBy($"id", $"tw")
          .select($"id", $"id".count, $"temp".avg, $"tw".end)
        // resultTable.toAppendStream[Row].print("result ")

        // SQL
        val resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) from sensors group by id, tumble(rt, interval '10' second)")
        // resultSqlTable.toRetractStream[Row].print("result sql ")

        // 5.2 Over Window
        // table API
        // val overResult = dataTable.window(Over partitionBy $"id" orderBy $"rt" preceding 2.rows as $"ow")
        // .select($("id"), $("rt"), $("id").count.over("ow"), $("temp").avg.over("ow"))
        // overResult.toAppendStream[Row].print("over ")

        // SQL
        val overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow from sensors window ow as (partition by id order by rt rows between 2 preceding and current row)")
        overSqlResult.toRetractStream[Row].print("over sql ")

        // 6. 执行
        env.execute("Table TimeAndWindow")
    }

    // 自定义 timestamp extractor
    class TimestampExtractor(maxOutOfOrderness: Time) extends BoundedOutOfOrdernessTimestampExtractor[Sensor](maxOutOfOrderness: Time) {

        override def extractTimestamp(element: Sensor): Long = element.timestamp * 1000
    }
}
