package com.hadoop.study.scala.example.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/24 16:11
 */

case class OrderEvent(orderId: Long, action: String, txId: String, timestamp: Long)
