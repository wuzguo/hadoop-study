package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/23 9:39
 */

case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, action: String, count: Long)
