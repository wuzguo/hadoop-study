package com.hadoop.study.fraud.detect.config

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/8 9:28
 */

case class Param[T](name: String, cls: Class[T], default: T) {
    override def toString: String = s"name: ${name}, default: ${default}"
}

object Param {

    def string(name: String, default: String): Param[String] = {
        Param(name, classOf[String], default)
    }

    def integer(name: String, default: Integer): Param[Integer] = {
        Param(name, classOf[Integer], default)
    }

    def long(name: String, default: Long): Param[Long] = {
        Param(name, classOf[Long], default)
    }

    def bool(name: String, default: Boolean): Param[Boolean] = {
        Param(name, classOf[Boolean], default)
    }
}
