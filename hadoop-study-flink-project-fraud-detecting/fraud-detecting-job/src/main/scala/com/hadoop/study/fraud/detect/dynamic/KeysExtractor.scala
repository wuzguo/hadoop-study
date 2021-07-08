package com.hadoop.study.fraud.detect.dynamic

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object KeysExtractor {

    import com.hadoop.study.fraud.detect.dynamic.FieldsExtractor

    /**
     * Extracts and concatenates field values by names.
     *
     * @param keyNames list of field names
     * @param object   target for values extraction
     */
    @throws[NoSuchFieldException]
    @throws[IllegalAccessException]
    def getKey(keyNames: Nothing, `object`: Any): String = {
        val sb = new StringBuilder
        sb.append("{")
        if (keyNames.size > 0) {
            val it = keyNames.iterator
            appendKeyValue(sb, `object`, it.next)
            while ( {
                it.hasNext
            }) {
                sb.append(";")
                appendKeyValue(sb, `object`, it.next)
            }
        }
        sb.append("}")
        sb.toString
    }

    @throws[IllegalAccessException]
    @throws[NoSuchFieldException]
    private def appendKeyValue(sb: StringBuilder, `object`: Any, fieldName: String): Unit = {
        sb.append(fieldName)
        sb.append("=")
        sb.append(FieldsExtractor.getFieldAsString(`object`, fieldName))
    }
}
