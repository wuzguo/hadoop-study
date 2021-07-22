package com.hadoop.study.spark.recomnend.producer

import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/22 9:55
 */

case class LogProcessor() extends Processor[String, String]() {

    private var context: ProcessorContext = _

    private val RATING_PREFIX = "product_rating_prefix:"

    override def init(processorContext: ProcessorContext): Unit = context = processorContext

    override def process(dummy: String, line: String): Unit = {

        // 提取数据，以固定前缀过滤日志信息
        if (line.contains(RATING_PREFIX)) {
            println(s"product rating data coming: ${line}")
            val value = line.split(RATING_PREFIX)(1).trim
            context.forward("logProcessor".getBytes, value.getBytes)
        }
    }

    override def punctuate(l: Long): Unit = {}

    override def close(): Unit = {}
}
