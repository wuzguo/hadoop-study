package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.dynamic.JsonMapper2
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:41
 */

case class JsonSerializer[T](sourceClass: Class[T]) extends RichFlatMapFunction[T, String] {

    private var parser: JsonMapper2[T] = _

    override def flatMap(value: T, out: Collector[String]): Unit = {
        val serialized = parser.to(value)
        out.collect(serialized)
    }

    override def open(parameters: Configuration): Unit = parser = JsonMapper2(sourceClass)
}
