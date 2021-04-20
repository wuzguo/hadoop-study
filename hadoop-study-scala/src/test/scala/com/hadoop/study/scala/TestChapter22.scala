package com.hadoop.study.scala

import com.hadoop.study.scala.chapter22.{Apple, Orange}
import org.junit.{FixMethodOrder, Test}

import scala.collection.mutable.ListBuffer

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 15:32
 */

@FixMethodOrder
class TestChapter22 {

    @Test
    def testList(): Unit = {
        val xs = List(1, 2, 3)
        println(xs)
        val ys: List[Any] = xs
        println(ys)
    }

    @Test
    def testList1(): Unit = {
        val apples = new Apple :: Nil
        println(apples)

        val fruits = new Orange :: apples
        println(fruits)

        val fruits2 = apples ::: new Orange :: Nil
        println(fruits2)
    }

    @Test
    def testListBuffer(): Unit = {

        // 低效
        val xs = List(1, 2, 3)
        var result = List[Int]()
        for (x <- xs) result = result ::: List(x + 1)
        println(result)

        // 高效
        val buf = new ListBuffer[Int]
        for (x <- xs) buf += x + 1
        println(buf.toList)
    }
}
