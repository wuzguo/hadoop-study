package com.hadoop.study.scala.chapter15

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 10:58
 */

case class BinOp(operator: String, left: Expr, right: Expr) extends Expr
