package com.hadoop.study.scala

import org.junit.{FixMethodOrder, Test}

@FixMethodOrder
class TestChapter2 {

    @Test
    def testVariable(): Unit = {
        val msg: String = "Hello, World"
        println(msg)
        var mag = "Hello, Tom"
        println(mag)

        var multiLine = "Hello," +
          " Tom"
        println(multiLine)
    }

    def max(x: Int, y: Int): Int = {
        if (x > y) x else y
    }


    def greet(): Unit = {
        println("Hello, Tom")
    }

    @Test
    def testFunction(): Unit = {
        println(max(1, 0))
        println(max(1, 10))
        println(greet())
    }

    def echoArgs(args: Array[String]): Unit = {
        var i = 0
        while (i < args.length) {
            if (i != 0)
                print(" ")
            print(args(i))
            i += 1
        }
        println("")
        println("-----------------------------------------")

        args.foreach(arg => println(arg))

        println("-----------------------------------------")
        for (arg <- args)
            println(arg)
    }

    @Test
    def testFunction2(): Unit = {
        echoArgs("Scala is even more fun".split(" "))
    }
}
