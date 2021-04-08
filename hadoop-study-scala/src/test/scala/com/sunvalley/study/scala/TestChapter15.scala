package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter15._
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 10:59
 */

@FixMethodOrder
class TestChapter15 {

    @Test
    def testCaseClass(): Unit = {
        val v = Var("x")
        println(v)
        println(v.name)
        println("-----------------------------------")

        // case class 的参数隐式获得 val 修饰符
        val op = BinOp("+", Number(1), v)
        println(op)
        println(op.left, op.right, op.operator)
        println("-----------------------------------")

        // 获取copy方法
        val op1 = op.copy(operator = "-")
        println(op1)
        println(op1.left, op1.right, op1.operator)
        println("-----------------------------------")
    }

    @Test
    def testSimplify(): Unit = {

        def simplifyTop(expr: Expr): Expr = expr match {
            case UnOp("-", UnOp("-", e)) => e // Double negation
            case BinOp("+", e, Number(0)) => e // Adding zero
            case BinOp("*", e, Number(1)) => e // Multiplying by one
            case _ => expr
        }

        println(simplifyTop(UnOp("-", UnOp("-", Var("x")))))


        def tupleDemo(expr: Any) = expr match {
            case (a, b, c) => println("matched", a, b, c)
            case _ =>
        }

        tupleDemo("a", 3, "-tuple")
    }

    @Test
    def testSimplifyAll(): Unit = {
        def simplifyAll(expr: Expr): Expr = expr match {
            case UnOp("-", UnOp("-", e)) =>
                simplifyAll(e) // `-' is its own inverse
            case BinOp("+", e, Number(0)) =>
                simplifyAll(e) // `0' is a neutral element for `+'
            case BinOp("*", e, Number(1)) =>
                simplifyAll(e) // `1' is a neutral element for `*'
            case UnOp(op, e) =>
                UnOp(op, simplifyAll(e))
            case BinOp(op, l, r) =>
                BinOp(op, simplifyAll(l), simplifyAll(r))
            case _ => expr
        }

        println(simplifyAll(UnOp("-", UnOp("-", Var("x")))))
    }

    @Test
    def testOption(): Unit = {
        val capitals = Map("France" -> "Pairs", "Japan" -> "Tokyo")
        // Some(Pairs)
        println(capitals get "France")
        // None
        println(capitals get "USA")

        def show(x: Option[String]) = x match {
            case Some(s) => s
            case None => "?"
        }
        // Pairs
        println(show(capitals get "France"))
        // ?
        println(show(capitals get "USA"))

        val withDefault: Option[Int] => Int = {
            case Some(x) => x
            case None => 0
        }

        // 10
        println(withDefault(Some(10)))
        // 0
        println(withDefault(None))
    }

    @Test
    def testLargerExample(): Unit = {

        val f = new ExprFormatter

        val e1 = BinOp("*", BinOp("/", Number(1), Number(2)),
            BinOp("+", Var("x"), Number(1)))

        val e2 = BinOp("+", BinOp("/", Var("x"), Number(2)),
            BinOp("/", Number(1.5), Var("x")))

        val e3 = BinOp("/", e1, e2)

        def show(e: Expr) = println(f.format(e) + "\n\n")

        for (e <- Array(e1, e2, e3)) show(e)
    }
}
