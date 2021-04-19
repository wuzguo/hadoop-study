package com.sunvalley.study.scala.chapter20

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:51
 */

class RationalClass(n: Int, d: Int) extends {
    val numerArg = n
    val denomArg = d
} with RationalTrait {
    def +(that: RationalClass) = new RationalClass(
        numer * that.denom + that.numer * denom,
        denom * that.denom
    )
}
