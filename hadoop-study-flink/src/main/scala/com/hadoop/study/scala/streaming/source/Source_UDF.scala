package com.hadoop.study.scala.streaming.source

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import scala.util.Random

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/9 21:29
 */

object Source_UDF {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        // 读取数据
        val udfStream = env.addSource(new CustomSourceFunction)
        udfStream.print("udf ")

        // 执行
        env.execute("Streaming Source UDF")
    }

    // 自定义数据源功能
    class CustomSourceFunction extends SourceFunction[Sensor] {

        private var running = true

        override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {
            val random = new Random()

            while (running) {
                val sensor = Sensor("sensor_" + random.nextInt(10), System.currentTimeMillis(), 30 + random.nextGaussian() * 20)
                ctx.collect(sensor)
                // 睡眠
                Thread.sleep(1000)
            }
        }

        override def cancel(): Unit = running = false
    }
}
