package com.hadoop.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:45
 */

trait Abstract {

    type T

    def transform(x: T): T

    val initial: T

    var current: T
}
