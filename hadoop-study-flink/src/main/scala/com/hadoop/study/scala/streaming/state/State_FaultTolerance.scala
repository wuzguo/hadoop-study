package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/10 15:42
 */

object State_FaultTolerance {

    def main(args: Array[String]): Unit = {
        // 设置环境变量
        System.setProperty("HADOOP_USER_NAME", "zak")

        // 环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置并行度
        env.setParallelism(1)

        import org.apache.flink.runtime.state.memory.MemoryStateBackend
        import org.apache.flink.streaming.api.CheckpointingMode
        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend)
        env.setStateBackend(new FsStateBackend("hdfs://hadoop001:9000/user/flink/state/fs"))
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop001:9000/user/flink/state/rocks"))

        // 2. 检查点配置
        env.enableCheckpointing(300)

        // 高级选项
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)

        // 从文件读取数据
        val socketStream = env.socketTextStream("hadoop003", 9999)

        // 构造Stream
        val sensorStream = socketStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        sensorStream.print("result ")

        env.execute("Streaming State FaultTolerance")
    }
}
