package com.hadoop.study.scala

import org.junit.{FixMethodOrder, Test}

import scala.collection.immutable.TreeMap

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 15:38
 */

@FixMethodOrder
class TestChapter17 {

    @Test
    def testList(): Unit = {
        val colors = List("red", "blue", "green")
        // (List(red, blue, green), red, List(blue, green))
        println(colors, colors.head, colors.tail)

        //  Array(0, 0, 0, 0, 0)
        val fiveInts = new Array[Int](5)
        // List(0, 0, 0, 0, 0)
        println(fiveInts.toList)
    }

    @Test
    def testArrays(): Unit = {
        val text = "See Spot run. Run, Spot. Run!"
        val wordsArray = text.split("[ !,.]+")
        println(wordsArray.toList)

        var tm = TreeMap(3 -> 'x', 1 -> 'x', 4 -> 'x')
        tm += (2 -> 'x')
        println(tm)
    }
}
