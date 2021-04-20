package com.hadoop.study.scala.chapter26

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:45
 */

object UpperCase {
    def unapply(s: String): Boolean = s.toUpperCase == s
}

