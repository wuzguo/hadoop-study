package com.hadoop.study.fraud.detect.utils

import com.hadoop.study.fraud.detect.beans.Transaction

import scala.reflect.runtime.universe.{TermName, Type, runtimeMirror, typeOf}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 16:20
 */

object FieldsExtractor {

    def getFieldAsString(value: Any, typeOf: Type, fieldName: String): String = {
        //获取对应的Mirrors,这里是运行时的
        val mirror = runtimeMirror(getClass.getClassLoader)
        //反射方法并调用
        val instanceMirror = mirror.reflect(value)
        //得到属性Field的Mirror
        val termSymbol = typeOf.decl(TermName(fieldName)).asTerm
        val filedMirror = instanceMirror.reflectField(termSymbol)
        filedMirror.get.toString
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

    def main(args: Array[String]): Unit = {

        val transaction = new Transaction()
        transaction.transactionId = 123L
        transaction.eventTime = System.currentTimeMillis()
        transaction.payeeId = 222
        transaction.beneficiaryId = 4444
        transaction.paymentAmount = BigDecimal(3)
        transaction.paymentType = "CRD"
        transaction.ingestionTimestamp = System.currentTimeMillis()

        val value = getFieldAsString(transaction, typeOf[Transaction], "paymentType")
        println(s"value: ${value}")
    }
}
