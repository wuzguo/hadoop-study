package com.hadoop.study.scala

import org.junit.{FixMethodOrder, Test}

import java.math.BigInteger
import scala.collection.mutable
import scala.io.Source


@FixMethodOrder
class TestChapter3 {

    @Test
    def testBigInteger(): Unit = {
        val bigInt = new BigInteger("1234")
        println(bigInt)
    }

    @Test
    def testGreetStrings(): Unit = {
        val greetStrings = new Array[String](3)
        greetStrings(0) = "Hello"
        greetStrings(1) = ", "
        greetStrings(2) = "world!\n"

        // to 2 包含2
        for (i <- 0 to 2)
            print(greetStrings(i))
    }

    @Test
    def testGreetStrings1(): Unit = {
        val greetStrings = new Array[String](3)
        greetStrings.update(0, "Hello")
        greetStrings.update(1, ", ")
        greetStrings.update(2, "world!\n")

        for (i <- 0.to(2))
            print(greetStrings.apply(i))
    }

    @Test
    def testArrays(): Unit = {
        val numNames = Array("zero", "one", "two")
        numNames.foreach(num => print(num + " "))
        println()
        println("------------------------------")
        val numNames2 = Array.apply("zero", "one", "two")
        for (i <- numNames2.indices)
            print(numNames2(i) + " ")
    }

    @Test
    def testList(): Unit = {
        val oneTwoThree = List(1, 2, 3)
        println(oneTwoThree)

        val oneTwo = List(1, 2)
        val threeFour = List(3, 4)
        val oneTwoThreeFour = oneTwo ::: threeFour
        println(oneTwo + " and " + threeFour + " were not mutated.")
        println("Thus, " + oneTwoThreeFour + " is a new list.")

        val twoThree = List(2, 3)
        val oneTwoThree1 = 1 :: 3 :: twoThree ::: oneTwo
        println(oneTwoThree1)

        val oneTwoThree2 = 1 :: 2 :: 3 :: Nil
        println(oneTwoThree2)

        val thrill = "Will" :: "fill" :: "until" :: Nil
        println(thrill)

        // List(List(a, b), List(c, d), Will, fill, until)
        val thrill2 = List("a", "b") :: List("c", "d") :: thrill
        println(thrill2)

        // List(a, b, c, d, Will, fill, until)
        val thrill3 = List("a", "b") ::: List("c", "d") ::: thrill
        println(thrill3)
    }


    @Test
    def testUseList(): Unit = {
        val thrill = "Will" :: "fill" :: "until" :: Nil
        // List(Will, fill, until)
        println(thrill)

        // 2
        println(thrill.count(s => s.length == 4))

        // List(until)
        println(thrill.drop(2))

        // List(Will)
        println(thrill.dropRight(2))

        // List(Will, fill)
        println(thrill.filter(s => s.length == 4))

        // true
        println(thrill.forall(s => s.endsWith("l")))

        // Will
        //fill
        //until
        thrill.foreach(s => println(s))

        // Willfilluntil
        thrill.foreach(print)
        println()
        // Will
        println(thrill.head)

        //  List(Will, fill)
        println(thrill.init)

        // false
        println(thrill.isEmpty)

        // until
        println(thrill.last)

        // 3
        println(thrill.length)

        // List(Willy, filly, untily)
        println(thrill.map(s => s + "y"))

        // Will, fill, until
        println(thrill.mkString(", "))

        // List(until)
        println(thrill.filterNot(s => s.length == 4))

        // List(until, fill, Will)
        println(thrill.reverse)

        // List(fill, until, Will)
        println(thrill.sortWith((s, t) => s.charAt(0).toLower < t.charAt(0).toLower))

        // List(fill, until)
        println(thrill.tail)
    }

    @Test
    def testTuple(): Unit = {
        val pair = (99, "Luftballons")
        println(pair.getClass)
        println(pair._1)
        println(pair._2)

        val pair2 = ('u', 'r', "the", 1, 4, "me")
        println(pair2._6)
        println(pair2.toString())

        var jetSet = Set("Boeing", "Airbus")
        jetSet += "Lear"
        println(jetSet.contains("Cessna"))
        println(jetSet.getClass)


        val treasureMap = mutable.Map[Int, String]()
        treasureMap += (1 -> "Go to island.")
        treasureMap += (2 -> "Find big X on ground.")
        treasureMap += (3 -> "Dig.")
        println(treasureMap(2))

    }

    def formatArgs(args: Array[String]) = args.mkString("\n")

    @Test
    def testFormatArgs(): Unit = {
        val res = formatArgs(Array("one", "two", "three"))
        println(res)
        assert(res == "one\ntwo\nthree")
    }

    def widthOfLength(s: String) = s.length.toString.length


    @Test
    def testSource() = {
        val  args: Array[String] = Array("D:\\logs\\test.log")
        if (args.length > 0) {
            val lines = Source.fromFile(args(0)).getLines().toList

            val longestLine = lines.reduceLeft(
                (a, b) => if (a.length > b.length) a else b
            )
            val maxWidth = widthOfLength(longestLine)

            for (line <- lines) {
                val numSpaces = maxWidth - widthOfLength(line)
                val padding = " " * numSpaces
                println(padding + line.length + " | " + line)
            }
        }
        else
            Console.err.println("Please enter filename")
    }
}
