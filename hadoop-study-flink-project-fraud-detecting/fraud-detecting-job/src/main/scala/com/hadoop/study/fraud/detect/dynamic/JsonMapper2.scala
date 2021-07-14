package com.hadoop.study.fraud.detect.dynamic

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 15:37
 */

case class JsonMapper2[T](target: Class[T]) {

    private val mapper =  JsonMapper.builder().addModule(DefaultScalaModule).build()

    def from(line: String): T = mapper.readValue(line, target)

    def to(value: T): String = mapper.writeValueAsString(value)
}
