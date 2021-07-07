package com.hadoop.study.scala.streaming.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/7 15:30
 */

case class Action(userId: Long, action: String) {

    override def toString: String = s"${userId}:${action}"
}
