package com.hadoop.study.scala.chapter18

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:05
 */

class Thermometer {

    var celsius: Float = _

    def fahrenheit = celsius * 9 / 5 + 32

    def fahrenheit_=(f: Float) = {
        celsius = (f - 32) * 5 / 9
    }

    override def toString = fahrenheit + "F/" + celsius + "C"
}
