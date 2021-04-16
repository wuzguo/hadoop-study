package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:46
 */

class Concrete extends Abstract {

    type T = String

    def transform(x: String) = x + x

    val initial = "hi"

    var current = initial
}
