package com.hadoop.study.fraud.detect.dynamic

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object FieldsExtractor {

    @throws[IllegalAccessException]
    @throws[NoSuchFieldException]
    def getFieldAsString(`object`: Any, fieldName: String): String = {
        val cls = `object`.getClass
        val field = cls.getField(fieldName)
        field.get(`object`).toString
    }

    @throws[NoSuchFieldException]
    @throws[IllegalAccessException]
    def getDoubleByName(fieldName: String, `object`: Any): Double = {
        val field = `object`.getClass.getField(fieldName)
        field.get(`object`).asInstanceOf[Double]
    }

    @throws[NoSuchFieldException]
    @throws[IllegalAccessException]
    def getBigDecimalByName(fieldName: String, `object`: Any): Nothing = {
        val field = `object`.getClass.getField(fieldName)
        new Nothing(field.get(`object`).toString)
    }

    @SuppressWarnings(Array("unchecked"))
    @throws[NoSuchFieldException]
    @throws[IllegalAccessException]
    def getByKeyAs[T](keyName: String, `object`: Any): T = {
        val field = `object`.getClass.getField(keyName)
        field.get(`object`).asInstanceOf[T]
    }
}
