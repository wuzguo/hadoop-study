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

    def getFieldAsString(instance: Any, typeOf: Type, fieldName: String): String = {
        reflectField(instance, typeOf, fieldName).toString
    }

    def getDoubleByName(instance: Any, typeOf: Type, fieldName: String): Double = {
        reflectField(instance, typeOf, fieldName).asInstanceOf[Double]
    }

    def getBigDecimalByName(instance: Any, typeOf: Type, fieldName: String): BigDecimal = {
        val value = reflectField(instance, typeOf, fieldName).toString
        BigDecimal(value)
    }

    def getByKeyAs[T](instance: Any, typeOf: Type, fieldName: String): T = {
        reflectField(instance, typeOf, fieldName).asInstanceOf[T]
    }

    /**
     * // https://www.jianshu.com/p/2f69ab68c60f
     * 反射获取值
     *
     * @param instance  实例对象
     * @param typeOf    类型
     * @param fieldName 字段名称
     * @return Any
     */
    private def reflectField(instance: Any, typeOf: Type, fieldName: String): Any = {
        //获取对应的Mirrors,这里是运行时的
        val mirror = runtimeMirror(getClass.getClassLoader)
        //反射方法并调用
        val instanceMirror = mirror.reflect(instance)
        //得到属性Field的Mirror
        val termSymbol = typeOf.decl(TermName(fieldName)).asTerm
        val filedMirror = instanceMirror.reflectField(termSymbol)

        filedMirror.get
    }
}
