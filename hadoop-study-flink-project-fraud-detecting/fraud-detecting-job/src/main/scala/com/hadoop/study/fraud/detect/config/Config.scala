package com.hadoop.study.fraud.detect.config

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 10:36
 */

case class Config() {

    private val values = new mutable.HashMap[Param[Any], Any]

    def this(inputParams: Parameters, stringParams: List[Param[String]], intParams: List[Param[Integer]], boolParams: List[Param[Boolean]]) {
        this()
        overrideDefaults(inputParams, stringParams)
        overrideDefaults(inputParams, intParams)
        overrideDefaults(inputParams, boolParams)
    }

    def put[T](key: Param[T], value: T): Unit = {
        values(key) = value
    }

    def get[T](param: Param[Any]): T = {
        values.get(param).asInstanceOf[T]
    }

    private def overrideDefaults[T](inputParams: Parameters, params: List[Param[T]]): Unit = {
        for (param <- params) {
            put(param, inputParams.getOrDefault(param))
        }
    }

}

object Config {

    def fromParameters(parameters: Parameters) = new Config(parameters, Parameters.STRING_PARAMS, Parameters.INT_PARAMS, Parameters.BOOL_PARAMS)
}