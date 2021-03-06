package com.hadoop.study.fraud.detect.config

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 10:36
 */

case class Config() {

    private val values = new mutable.HashMap[Any, Any]

    def this(inputParams: Parameters, stringParams: List[Param[String]], intParams: List[Param[Integer]], boolParams: List[Param[Boolean]]) {
        this()
        overrideDefaults(inputParams, stringParams)
        overrideDefaults(inputParams, intParams)
        overrideDefaults(inputParams, boolParams)
    }

    private def overrideDefaults[T](inputParams: Parameters, params: List[Param[T]]): Unit = {
        for (param <- params) {
            put(param, inputParams.getOrDefault(param))
        }
    }

    def put[T](key: Param[T], value: T): Unit = {
        values(key) = value
    }

    def get[T](key: Param[T]): T = {
        key.cls.cast(values(key))
    }

    override def toString: String = {
        val buf = ListBuffer[String]()
        values.toList.foreach(some => buf.append(s"{key=[${some._1}], value=[${some._2}]}"))
        buf.toString()
    }
}

object Config {
    def from(parameters: Parameters) = new Config(parameters, Parameters.STRING_PARAMS, Parameters.INT_PARAMS, Parameters.BOOL_PARAMS)
}