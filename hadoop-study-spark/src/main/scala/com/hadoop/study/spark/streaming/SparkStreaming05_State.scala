package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 15:12
 */

object SparkStreaming05_State {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_State$")

        val ssc = new StreamingContext(conf, Seconds(3))
        // 设置检查点
        // 3. 异常处理
        // 如果出现： boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)
        // 本地Hadoop_Home/bin 目录下的 hadoop.dll 文件缺失
         ssc.checkpoint("streaming_cp")

        // 无状态数据操作，只对当前的采集周期内的数据进行处理
        // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
        // 使用有状态操作时，需要设定检查点路径
        val lines = ssc.socketTextStream("hadoop002", 9999)

        // 切分字符串
        val words = lines.flatMap(_.split(" "))

        val wordToOne = words.map((_, 1))

        //val wordToCount = wordToOne.reduceByKey(_+_)
        // updateStateByKey：根据key对数据的状态进行更新
        // 传递的参数中含有两个值
        // 第一个值表示相同的key的value数据
        // 第二个值表示缓存区相同key的value数据
        val state = wordToOne.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
            val newCount = buff.getOrElse(0) + seq.sum
            Option(newCount)
        })
        // 打印
        state.print()

        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
