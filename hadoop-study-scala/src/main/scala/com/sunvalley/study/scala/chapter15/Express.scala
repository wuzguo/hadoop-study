package com.sunvalley.study.scala.chapter15

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 11:46
 */

object Express extends App {

    val f = new ExprFormatter

    val e1 = BinOp("*", BinOp("/", Number(1), Number(2)),
        BinOp("+", Var("x"), Number(1)))

    val e2 = BinOp("+", BinOp("/", Var("x"), Number(2)),
        BinOp("/", Number(1.5), Var("x")))

    val e3 = BinOp("/", e1, e2)

    def show(e: Expr) = println(f.format(e) + "\n\n")

    for (e <- Array(e1, e2, e3)) show(e)
}
