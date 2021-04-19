package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:52
 */

trait LazyRationalTrait {

    val numerArg: Int
    val denomArg: Int

    lazy val numer = numerArg / g
    lazy val denom = denomArg / g

    override def toString = numer + "/" + denom

    private lazy val g = {
        require(denomArg != 0)
        gcd(numerArg, denomArg)
    }

    private def gcd(a: Int, b: Int): Int =
        if (b == 0) a else gcd(b, a % b)
}

