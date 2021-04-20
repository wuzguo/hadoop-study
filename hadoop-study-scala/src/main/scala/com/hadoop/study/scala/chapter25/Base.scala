package com.hadoop.study.scala.chapter25

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:07
 */

abstract class Base


object Base {
    val fromInt: Int => Base = Array(A, T, G, U)
    val toInt: Base => Int = Map(A -> 0, T -> 1, G -> 2, U -> 3)
}
