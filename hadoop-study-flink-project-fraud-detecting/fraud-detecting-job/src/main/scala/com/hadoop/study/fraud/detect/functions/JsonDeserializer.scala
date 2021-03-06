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
 * @date 2021/7/8 15:35
 */

case class JsonDeserializer[T](target: Class[T]) extends RichFlatMapFunction[String, T] {

    private var mapper: JsonMapper2[T] = _

    override def flatMap(value: String, out: Collector[T]): Unit = {
        val parsed = mapper.from(value)
        out.collect(parsed)
    }

    override def open(parameters: Configuration): Unit = mapper = JsonMapper2(target)
}
