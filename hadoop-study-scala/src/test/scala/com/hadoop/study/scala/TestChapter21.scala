package com.hadoop.study.scala

import com.hadoop.study.scala.chapter21.{Greeter, Greeter2, PreferredPrompt}
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 14:25
 */

@FixMethodOrder
class TestChapter21 {

    @Test
    def tetImplicit(): Unit = {

        //   var i: Int = 2.5;
        //   println(i)

        implicit def doubleToInt(x: Double) = x.toInt

        var n: Int = 3.5
        // 3
        println(n)
    }

    @Test
    def testGreet(): Unit = {
        val bobsPrompt = new PreferredPrompt("relax> ")
        Greeter.greet("Bob")(bobsPrompt)

        import com.hadoop.study.scala.chapter21.JoesPrefs._
        Greeter.greet("Joe")


        Greeter2.greet("Bob")(prompt, drink)
        Greeter2.greet("Bob")(bobsPrompt, drink)
        // Greeter2.greet("Bob")(prompt)
        // Greeter2.greet("Bob")(drink)
        Greeter2.greet("Bob")
    }


    def maxListOrdering[T](elements: List[T])(ordering: Ordering[T]): T =
        elements match {
            case List() =>
                throw new IllegalArgumentException("empty list!")
            case List(x) => x
            case x :: rest =>
                val maxRest = maxListOrdering(rest)(ordering)
                if (ordering.gt(x, maxRest)) x
                else maxRest
        }

    def maxListImpParam[T](elements: List[T])(implicit ordering: Ordering[T]): T =
        elements match {
            case List() =>
                throw new IllegalArgumentException("empty list!")
            case List(x) => x
            case x :: rest =>
                val maxRest = maxListImpParam(rest)(ordering)
                if (ordering.gt(x, maxRest)) x
                else maxRest
        }

    @Test
    def testMaxList(): Unit = {
        println(maxListImpParam(List(1,5,10,3)))
        println(maxListImpParam(List(1.5, 5.2, 10.7, 3.14159)))
        println( maxListImpParam(List("one", "two", "three")))
    }
}
