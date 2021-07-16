package com.hadoop.study.fraud.detect.functions

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.hadoop.study.fraud.detect.sources.BaseGenerator
import org.apache.flink.configuration.Configuration

import java.text.SimpleDateFormat
import java.time.ZoneOffset
import java.util.{SplittableRandom, TimeZone}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:46
 */

case class JsonGeneratorWrapper[T](generator: BaseGenerator[T]) extends BaseGenerator[String] {

    private var mapper: ObjectMapper = _

    override def randomEvent(splitRandom: SplittableRandom, id: Long): String = {
        val transaction = generator.randomEvent(splitRandom, id)
        mapper.writeValueAsString(transaction)
    }

    def init(mapper: ObjectMapper): ObjectMapper = {
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
        mapper.setSerializationInclusion(Include.NON_NULL)
        // 不知道的属性，不异常
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
        mapper.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC))
        mapper.findAndRegisterModules
        mapper
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        mapper = init(new ObjectMapper())
    }
}
