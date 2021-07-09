package com.hadoop.study.fraud.detect.utils

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object FieldsExtractor {

    def getFieldAsString(value: Any, fieldName: String): String = {
        val cls = value.getClass
        val field = cls.getField(fieldName)
        field.get(value).toString
    }

    def getDoubleByName(fieldName: String, value: Any): Double = {
        val field = value.getClass.getField(fieldName)
        field.get(value).asInstanceOf[Double]
    }

    def getBigDecimalByName(fieldName: String, value: Any): BigDecimal = {
        val field = value.getClass.getField(fieldName)
        BigDecimal(field.get(value).toString)
    }

    def getByKeyAs[T](keyName: String, value: Any): T = {
        val field = value.getClass.getField(keyName)
        field.get(value).asInstanceOf[T]
    }
}
