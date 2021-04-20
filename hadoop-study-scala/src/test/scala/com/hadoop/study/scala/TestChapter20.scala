package com.hadoop.study.scala

import com.hadoop.study.scala.chapter20._
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/8 16:44
 */

@FixMethodOrder
class TestChapter20 {

    @Test
    def testRationalTrait(): Unit = {
        // java.lang.IllegalArgumentException: requirement failed
        //        val rational = new RationalTrait {
        //            val numerArg: Int = 10
        //            val denomArg: Int = 20
        //        }
        //        println(rational)

        val rational1 = new {
            val numerArg = 1
            val denomArg = 2
        } with RationalTrait
        // 1/2
        println(rational1)

        // 1/2
        println(new LazyRationalTrait {
            val numerArg = 1
            val denomArg = 2
        })
    }

    @Test
    def testEnumeration(): Unit = {
        // North East South West
        for (d <- Direction.values)
            print(d + " ")

        //  1
        println(Direction.East.id)
        // East
        println(Direction(1))
    }

    @Test
    def testCurrency(): Unit = {
        val res = Japan.Yen from US.Dollar * 100
        // 12110 JPY
        println(Japan.Yen from US.Dollar * 100)

        val res1 = Europe.Euro from res
        // 75.95 EUR
        println(res1)

        val res2 = US.Dollar from res1
        // 99.95 USD
        println(res2)

        val res3 = US.Dollar * 100 + res2
        // 199.95 USD
        println(res3)

        // error: type mismatch;
        // println(US.Dollar + Europe.Euro)
    }
}
