package com.hadoop.study.fraud.detect.dynamic

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.IOException

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:37
 */

case class JsonMapper[T](targetClass: Class[T]) {

    private val objectMapper: ObjectMapper = new ObjectMapper()

    @throws[IOException]
    def from(line: String): T = objectMapper.readValue(line, targetClass)

    @throws[IOException]
    def to(line: T): String = objectMapper.writeValueAsString(line)
}
