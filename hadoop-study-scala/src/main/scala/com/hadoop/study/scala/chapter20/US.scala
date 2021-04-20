package com.hadoop.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 17:21
 */

object US extends CurrencyZone {

    abstract class Dollar extends AbstractCurrency {
        def designation = "USD"
    }

    type Currency = Dollar

    def make(cents: Long) = new Dollar {
        val amount = cents
    }

    val Cent = make(1)

    val Dollar = make(100)

    val CurrencyUnit = Dollar
}