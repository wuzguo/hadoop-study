package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 17:23
 */

object Japan extends CurrencyZone {
    abstract class Yen extends AbstractCurrency {
        def designation = "JPY"
    }

    type Currency = Yen

    def make(yen: Long) = new Yen {
        val amount = yen
    }

    val Yen = make(1)
    val CurrencyUnit = Yen
}
