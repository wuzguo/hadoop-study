package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 17:23
 */

object Europe extends CurrencyZone {
    abstract class Euro extends AbstractCurrency {
        def designation = "EUR"
    }
    type Currency = Euro
    def make(cents: Long) = new Euro {
        val amount = cents
    }
    val Cent = make(1)
    val Euro = make(100)
    val CurrencyUnit = Euro
}

