package com.hadoop.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 17:16
 */

abstract class Currency {

    val amount: Long

    def designation: String

    override def toString = amount + " " + designation

    def +(that: Currency): Currency

    def *(x: Double): Currency
}
