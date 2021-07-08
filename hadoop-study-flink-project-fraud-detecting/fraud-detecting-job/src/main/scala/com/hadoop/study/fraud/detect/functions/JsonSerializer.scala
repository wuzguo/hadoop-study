package com.hadoop.study.fraud.detect.functions

import com.hadoop.study.fraud.detect.dynamic.JsonMapper
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.Logger

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:41
 */

case class JsonSerializer[T](sourceClass: Class[T], log: Logger) extends RichFlatMapFunction[T, String] {

    private var parser: JsonMapper[T] = _

    override def flatMap(value: T, out: Collector[String]): Unit = {
        try {
            log.trace("{}", value)
            val serialized = parser.to(value)
            out.collect(serialized)
        } catch {
            case e: Exception => log.warn("Failed serializing {} to JSON, dropping it: ", sourceClass, e)
        }
    }

    override def open(parameters: Configuration): Unit = parser = JsonMapper(sourceClass)
}
