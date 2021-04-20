package com.hadoop.study.scala

import org.junit.{FixMethodOrder, Test}


@FixMethodOrder
class TestChapter8 {

    @Test
    def testFunction(): Unit = {
        val increase = (x: Int) => x + 1
        println(increase(1))

        val someNumbers = List(-11, -10, -5, 0, 5, 10)
        println(someNumbers.filter(x => x > 0))
        println("-------------------------")

        someNumbers.foreach(x => println(x + 1))
        println("-------------------------")

        someNumbers.map(increase).foreach(x => println(x))
        println("-------------------------")

        someNumbers.map(increase).foreach(println)
        println("-------------------------")

        someNumbers.map(increase).foreach(println _)
        println("-------------------------")

        println(someNumbers.filter(_ > 0))

        println("-------------------------")
        val f = (_: Int) + (_: Int)
        println(f)
        println(f(1, 3))

        println("-------------------------")

        def sum(a: Int, b: Int, c: Int) = a + b + c

        println(sum(1, 2, 3))

        println("-------------------------")
        val sum1 = (a: Int, b: Int, c: Int) => a + b + c
        println(sum1(1, 2, 3))

        println("-------------------------")
        val a = sum _
        println(a(1, 2, 3))

        println("-------------------------")
        val b = sum(_, _, 3)
        println(b(1, 2))
    }

    @Test
    def testClosure(): Unit = {
        val someNumbers = List(-11, -10, -5, 0, 5, 10)
        var sum = 0
        someNumbers.foreach(sum += _)
        println(sum)
        println("-------------------------")

        // 闭包
        def makeIncreaser(more: Int) = (x: Int) => x + more

        def inc1 = makeIncreaser(1)

        println(inc1(100))

        println("-------------------------")

        def inc999 = makeIncreaser(999)

        println(inc999(100))
    }

    @Test
    def testSpecialFunction(): Unit = {

        def echo(args: String*) = for (arg <- args) println(arg)

        echo("hello")
        echo("hello", "tomcat")
        echo("hello", "tomcat", "function")

        val arr = Array("What's", "up", "doc?")
        echo(arr: _*)
        println("-------------------------")

        def speed(distance: Float, time: Float) = distance / time

        println(speed(100, 10))

        def printTime(out: java.io.PrintStream = Console.out) =
            out.println("time = " + System.currentTimeMillis())

        def printTime2(out: java.io.PrintStream = Console.out,
                       divisor: Int = 1) =
            out.println("time = " + System.currentTimeMillis() / divisor)

        printTime()
        println("-------------------------")
        printTime2(divisor = 1000)
        println("-------------------------")
        printTime2(out = Console.err)
    }

    @Test
    def testTailRecursion(): Unit = {
        def isEven(x: Int): Boolean =
            if (x == 0) true else isOdd(x - 1)

        def isOdd(x: Int): Boolean =
            if (x == 0) false else isEven(x - 1)

        //  val funValue = nestedFun _

        def nestedFun(x: Int): Unit = {
            if (x != 0) {
                println(x)
                nestedFun(x - 1)
            }
        }

        println(nestedFun(1))
        println(isEven(3))
    }
}
