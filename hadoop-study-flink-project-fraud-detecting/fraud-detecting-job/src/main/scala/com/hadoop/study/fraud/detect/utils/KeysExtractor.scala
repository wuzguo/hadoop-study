package com.hadoop.study.fraud.detect.utils

import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object KeysExtractor {

    /**
     * Extracts and concatenates field values by names.
     *
     * @param keyNames list of field names
     * @param value    target for values extraction
     */
    def getKey(keyNames: List[String], value: Any): String = {
        val buffer: ListBuffer[String] = ListBuffer()

        buffer.append("{")
        if (keyNames.nonEmpty) {
            val it = keyNames.iterator
            appendKeyValue(buffer, value, it.next)
            while (it.hasNext) {
                buffer.append(";")
                appendKeyValue(buffer, value, it.next)
            }
        }
        buffer.append("}")
        buffer.toString
    }

    private def appendKeyValue(buffer: ListBuffer[String], value: Any, fieldName: String): Unit = {
        buffer.append(fieldName)
        buffer.append("=")
        buffer.append(FieldsExtractor.getFieldAsString(value, fieldName))
    }
}
