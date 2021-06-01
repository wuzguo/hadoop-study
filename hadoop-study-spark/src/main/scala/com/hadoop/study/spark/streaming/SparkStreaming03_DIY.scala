package com.hadoop.study.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/1 13:49
 */

object SparkStreaming03_DIY {

    def main(args: Array[String]): Unit = {
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_DIY")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(conf, Seconds(3))
        val messageDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver())

        // 打印数据
        messageDStream.print()

        // 1. 启动采集器
        ssc.start()

        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

    /*
      自定义数据采集器
      1. 继承Receiver，定义泛型, 传递参数
      2. 重写方法
    */
    class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

        private var flg = true

        override def onStart(): Unit = {
            new Thread(() => {
                while (flg) {
                    val message = "采集的数据为：" + new Random().nextInt(10).toString
                    store(message)
                    Thread.sleep(500)
                }
            }).start()
        }

        override def onStop(): Unit = {
            flg = false
        }
    }
}
