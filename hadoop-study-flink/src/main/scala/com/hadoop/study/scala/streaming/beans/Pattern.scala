package com.hadoop.study.scala.streaming.beans

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/7 15:31
 */

case class Pattern(preAction: String, action: String) {

    override def toString: String = s"${preAction}:${action}"
}
