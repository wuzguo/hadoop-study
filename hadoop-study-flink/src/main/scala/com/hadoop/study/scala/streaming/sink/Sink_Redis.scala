package com.hadoop.study.scala.streaming.sink

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 9:28
 */

object Sink_Redis {

    def main(args: Array[String]): Unit = {
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

        // 配置文件
        val config = new FlinkJedisPoolConfig.Builder()
          .setHost("hadoop002")
          .setDatabase(0)
          .setPort(6379)
          .build()

        sensorStream.addSink(new RedisSink(config, new CustomRedisMapper))
        // 执行
        env.execute("Streaming Sink Redis")
    }

    // 自定义Mapper
    class CustomRedisMapper extends RedisMapper[Sensor] {
        override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,
            "sink:sensor:")

        override def getKeyFromData(data: Sensor): String = data.id

        override def getValueFromData(data: Sensor): String = data.toString
    }
}
