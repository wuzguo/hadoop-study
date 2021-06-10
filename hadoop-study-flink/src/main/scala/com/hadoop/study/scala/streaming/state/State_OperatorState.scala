package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.util

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 15:42
 */

object State_OperatorState {

    def main(args: Array[String]): Unit = {
        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 从文件读取数据
        val socketStream = env.socketTextStream("hadoop003", 9999)

        // 构造Stream
        val sensorStream = socketStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        // 定义一个有状态的map操作，统计当前分区数据个数
        sensorStream.map(new CustomCountMapper).print()

        env.execute("Streaming State OperatorState")
    }

    class CustomCountMapper extends MapFunction[Sensor, Int] with ListCheckpointed[Integer] {

        var count = 0

        override def map(value: Sensor): Int = {
            count += 1
            count
        }

        override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
            new util.ArrayList[Integer](count)
        }

        override def restoreState(state: util.List[Integer]): Unit = {
            import scala.collection.JavaConversions._
            for (num <- state) {
                count += num
            }
        }
    }
}
