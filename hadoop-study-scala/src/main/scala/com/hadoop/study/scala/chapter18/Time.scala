package com.sunvalley.study.scala.chapter18

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:05
 */

class Time {

    private[this] var h = 12
    private[this] var m = 0

    def hour: Int = h

    def hour_=(x: Int) = {
        h = x
    }

    def minute: Int = m

    def minute_=(x: Int) = {
        m = x
    }
}
