package com.hadoop.study.scala.chapter28

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 20:13
 */

abstract class CCTherm {
    val description: String
    val yearMade: Int
    val dateObtained: String
    val bookPrice: Int // in US cents
    val purchasePrice: Int // in US cents
    val condition: Int // 1 to 10

    override def toString = description
}