package com.hadoop.study.scala.streaming.state

import com.hadoop.study.scala.streaming.beans.Sensor
import org.apache.flink.api.common.restartstrategy.RestartStrategies
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
        // 内存级别 存储在TaskManager的JVM堆上
        env.setStateBackend(new MemoryStateBackend)
        // 存到远程的持久化文件系统（ FileSystem ）
        env.setStateBackend(new FsStateBackend("hdfs://hadoop001:9000/user/flink/state/fs"))
        // 将所有状态序列化后，存入本地的 RocksDB 中存储
        env.setStateBackend(new RocksDBStateBackend(""))

        // 2. 检查点配置
        env.enableCheckpointing(300)

        // 高级选项
        // 检查点执行模式
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        // 执行超时时间
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        // 并行执行的数量
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        // 两次执行的最小间隔时间
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
        // 可容忍的检查点失败次数，默认值为 0 表示我们不容忍任何检查点失败
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)

        // 3. 重启策略配置 // 固定延迟重启（隔一段时间尝试重启一次）
        // (尝试重启次数， 尝试重启的时间间隔)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart( 3, 60000))

        // 从文件读取数据
        val socketStream = env.socketTextStream("hadoop003", 9999)

        // 构造Stream
        val sensorStream = socketStream.map(line => {
            val values = line.split(",")
            Sensor(values(0), values(1).trim.toLong, values(2).trim.toDouble)
        })

        sensorStream.print("result ")

        // 执行
        env.execute("Streaming State FaultTolerance")
    }
}
