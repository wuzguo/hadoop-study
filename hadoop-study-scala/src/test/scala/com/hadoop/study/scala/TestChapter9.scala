package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter9.{FileMatcher, FileMatcher2, FileMatcher3, FileMatcher4, FileMatcher5}
import org.junit.{FixMethodOrder, Test}

import java.io.{File, PrintWriter}
import java.util.Date

@FixMethodOrder
class TestChapter9 {

    @Test
    def testReducingCode(): Unit = {
        FileMatcher.filesEnding(".xml").foreach(println)
        FileMatcher.filesContaining(".xml").foreach(println)
        FileMatcher.filesRegex("(xml)").foreach(println)
        println("-----------------------------------")

        FileMatcher2.filesEnding(".xml").foreach(println)
        FileMatcher2.filesContaining(".xml").foreach(println)
        FileMatcher2.filesRegex("(xml)").foreach(println)

        println("-----------------------------------")

        FileMatcher3.filesEnding(".xml").foreach(println)
        FileMatcher3.filesContaining(".xml").foreach(println)
        FileMatcher3.filesRegex("(xml)").foreach(println)

        println("-----------------------------------")

        FileMatcher4.filesEnding(".xml").foreach(println)
        FileMatcher4.filesContaining(".xml").foreach(println)
        FileMatcher4.filesRegex("(xml)").foreach(println)

        println("-----------------------------------")

        FileMatcher5.filesEnding(".xml").foreach(println)
        FileMatcher5.filesContaining(".xml").foreach(println)
        FileMatcher5.filesRegex("(xml)").foreach(println)
    }

    @Test
    def testSimplifying(): Unit = {

        def containsNeg(nums: List[Int]): Boolean = {
            var exists = false
            for (num <- nums)
                if (num < 0) exists = true
            exists
        }

        def containsNeg2(nums: List[Int]) = nums.exists(num => num < 0)

        def containsNeg3(nums: List[Int]) = nums.exists(_ < 0)

        println(containsNeg(List(1, 2, 3, 4)))
        println(containsNeg(List(1, 2, -3, -4)))
        println("-----------------------------------")
        println(containsNeg2(List(1, 2, 3, 4)))
        println(containsNeg2(List(1, 2, -3, -4)))
        println("-----------------------------------")
        println(containsNeg3(List(1, 2, 3, 4)))
        println(containsNeg3(List(1, 2, -3, -4)))
    }

    @Test
    def testCurrying(): Unit = {

        def plainOldSum(x: Int, y: Int) = x + y

        // 柯里化，多个参数列表
        def curriedSum(x: Int)(y: Int) = x + y

        println(plainOldSum(1, 2))
        println("-----------------------------------")
        println(curriedSum(1)(2))
        println("-----------------------------------")

        def first(x: Int) = (y: Int) => x + y

        def second = first(1)

        println(second(2))
        println("-----------------------------------")

        val onePlus = curriedSum(1) _
        println(onePlus(2))
        println("-----------------------------------")

        val twoPlus = curriedSum(2) _
        println(onePlus(2))
        println("-----------------------------------")
    }

    @Test
    def testControlStructures(): Unit = {

        def twice(op: Double => Double, x: Double) = op(op(x))
        // 5
        println(twice(_ + 1, 3))
        println("-----------------------------------")


        def withPrintWriter(file: File, op: PrintWriter => Unit) = {
            val writer = new PrintWriter(file)
            try {
                op(writer)
            } finally {
                writer.close()
            }
        }
        // 创建文件
        withPrintWriter(
            new File("date.txt"),
            writer => writer.println(new Date)
        )

        // 贷出模式，只对单个参数有效
        println("Hello Tomcat")
        println {
            "Hello Tomcat"
        }

        // 贷出模式改造
        def withPrintWriter2(file: File)(op: PrintWriter => Unit) = {
            val writer = new PrintWriter(file)
            try {
                op(writer)
            } finally {
                writer.close()
            }
        }

        // 方法1
        withPrintWriter2 {
            new File("date2.txt")
        } {
            writer => writer.println(new Date)
        }

        // 方法2
        withPrintWriter2(new File("date3.txt")) {
            writer => writer.println(new Date)
        }
    }

    @Test
    def testByNameParameter(): Unit = {

        val assertionsEnabled = true

        def myAssert(predicate: () => Boolean) =
            if (assertionsEnabled && !predicate())
                throw new AssertionError

        myAssert(() => 5 > 3)

        // 传名参数
        def byNameAssert(predicate: => Boolean) =
            if (assertionsEnabled && !predicate)
                throw new AssertionError

        byNameAssert(5 > 3)
    }


}
