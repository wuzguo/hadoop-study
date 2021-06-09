package com.hadoop.study.scala.streaming.source

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 20:58
 */

object Streaming_Collection {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从集合中读取数据
        val sensorStream: DataStream[Sensor] = env.fromCollection(List(
            Sensor("sensor_1", 1547718199L, 35.8),
            Sensor("sensor_6", 1547718201L, 15.4),
            Sensor("sensor_7", 1547718202L, 6.7),
            Sensor("sensor_10", 1547718205L, 38.1)
        ))
        // 打印数据
        sensorStream.print("sensor ")

        // 数字
        val intStream = env.fromElements(1, 2, 4, 67, 189)
        // 打印数据
        intStream.print("int ")

        // 执行
        env.execute("Streaming Source Collection")
    }
}
