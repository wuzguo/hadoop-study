package com.hadoop.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:48
 */

trait RationalTrait {

    val numerArg: Int

    val denomArg: Int

    require(denomArg != 0)

    private val g = gcd(numerArg, denomArg)

    val numer = numerArg / g
    val denom = denomArg / g

    private def gcd(a: Int, b: Int): Int =
        if (b == 0) a else gcd(b, a % b)

    override def toString = numer + "/" + denom
}
