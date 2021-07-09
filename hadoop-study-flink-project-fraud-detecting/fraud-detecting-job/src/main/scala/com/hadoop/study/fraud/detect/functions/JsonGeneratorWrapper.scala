package com.hadoop.study.fraud.detect.functions

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.hadoop.study.fraud.detect.sources.BaseGenerator

import java.util.SplittableRandom

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:46
 */

case class JsonGeneratorWrapper[T](wrappedGenerator: BaseGenerator[T]) extends BaseGenerator[String] {

    private val objectMapper = new ObjectMapper()

    override def randomEvent(splitRandom: SplittableRandom, id: Long): String = {
        val transaction = wrappedGenerator.randomEvent(splitRandom, id)

        var json: String = null
        try json = objectMapper.writeValueAsString(transaction)
        catch {
            case e: JsonProcessingException => throw new RuntimeException(e)
        }
        json
    }
}
