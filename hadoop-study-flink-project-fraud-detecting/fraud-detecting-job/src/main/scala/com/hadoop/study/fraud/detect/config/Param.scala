package com.hadoop.study.fraud.detect.config

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 9:28
 */

case class Param[T](name: String, cls: Class[T], default: T)


object Param {

    def string(name: String, default: String): Param[String] = {
        Param(name, classOf[String], default = default)
    }

    def integer(name: String, default: Integer): Param[Integer] = {
        Param(name, classOf[Integer], default)
    }

    def bool(name: String, default: Boolean): Param[Boolean] = {
        Param(name, classOf[Boolean], default)
    }
}
