package com.hadoop.study.fraud.detect.utils

import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.Type

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object KeysExtractor {

    private val log = LoggerFactory.getLogger("KeysExtractor")

    /**
     * Extracts and concatenates field values by names.
     *
     * @param keyNames list of field names
     * @param value    target for values extraction
     */
    def getKey(value: Any, typeOf: Type, keyNames: List[String]): String = {
        val builder = new StringBuilder
        builder.append("{")
        if (keyNames.nonEmpty) {
            val it = keyNames.iterator
            appendKeyValue(value, typeOf, it.next, builder)
            while (it.hasNext) {
                builder.append(";")
                appendKeyValue(value, typeOf, it.next, builder)
            }
        }
        builder.append("}")
        log.info("keys extractor getKey: {}", builder.toString)
        builder.toString
    }

    private def appendKeyValue(value: Any, typeOf: Type, fieldName: String, buffer: StringBuilder): Unit = {
        buffer.append(fieldName)
        buffer.append("=")
        buffer.append(FieldsExtractor.getStringByName(value, typeOf, fieldName))
    }
}
