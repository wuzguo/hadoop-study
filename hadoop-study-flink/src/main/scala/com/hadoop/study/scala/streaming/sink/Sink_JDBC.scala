package com.hadoop.study.scala.streaming.sink

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 10:07
 */

object Sink_JDBC {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val fileStream = env.readTextFile("./hadoop-study-datas/flink/core/sensor.txt")

        // 构造Stream
        val sensorStream = fileStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        sensorStream.addSink(new JDBCSinkFunction)

        // 执行
        env.execute("Streaming Sink JDBC")
    }

    class JDBCSinkFunction extends RichSinkFunction[Sensor] {

        var connection: Connection = _
        var updateStmt: PreparedStatement = _
        var insertStmt: PreparedStatement = _

        override def open(parameters: Configuration): Unit = {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop003:3306/flink-sink", "root", "123456")
            updateStmt = connection.prepareStatement("update sink_sensor set temp = ? where id = ?")
            insertStmt = connection.prepareStatement("insert into sink_sensor (id,temp,timestamp) values (?, ?, ?)")
        }

        override def invoke(value: Sensor, context: SinkFunction.Context): Unit = {
            updateStmt.setDouble(1, value.temp)
            updateStmt.setString(2, value.id)
            updateStmt.execute()

            // 如果为0 表示是新增
            if (updateStmt.getUpdateCount == 0) {
                insertStmt.setString(1, value.id)
                insertStmt.setDouble(2, value.temp)
                insertStmt.setLong(3, value.timestamp)
                insertStmt.execute()
            }
        }

        override def close(): Unit = {
            updateStmt.close()
            insertStmt.close()
            connection.close()
        }
    }
}
