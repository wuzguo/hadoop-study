package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 17:29
 */

object Converter {

    var exchangeRate = Map(
        "USD" -> Map("USD" -> 1.0, "EUR" -> 0.7596,
            "JPY" -> 1.211, "CHF" -> 1.223),
        "EUR" -> Map("USD" -> 1.316, "EUR" -> 1.0,
            "JPY" -> 1.594, "CHF" -> 1.623),
        "JPY" -> Map("USD" -> 0.8257, "EUR" -> 0.6272,
            "JPY" -> 1.0, "CHF" -> 1.018),
        "CHF" -> Map("USD" -> 0.8108, "EUR" -> 0.6160,
            "JPY" -> 0.982, "CHF" -> 1.0)
    )
}