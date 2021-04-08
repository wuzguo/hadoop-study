package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter12._
import com.sunvalley.study.scala.chapter6.Rational
import org.junit.{FixMethodOrder, Test}

@FixMethodOrder
class TestChapter12 {

    @Test
    def testTraitsWork(): Unit = {
        val frog = new Frog
        println(frog)
        frog.philosophize()

        val philosophical = new Philosophical {

        }
        println(philosophical)
        println(philosophical.philosophize())
    }

    @Test
    def testRectangular(): Unit = {
        val rect = new Rectangle(new Point(1, 1), new Point(10, 10))
        println(rect.left, rect.right, rect.width)
    }

    @Test
    def testOrdered(): Unit = {
        val half = new Rational(1, 2)
        val third = new Rational(1, 3)
        println(half < third)
        println(half > third)
    }

    @Test
    def testBasicIntQueue(): Unit = {
        class MyQueue extends BasicIntQueue with Doubling
        val queue = new MyQueue
        println(queue)
        queue.put(10)
        println(queue.get())
        println("-------------------------------------------")

        val queue1 = new BasicIntQueue with Doubling
        println(queue1)
        queue1.put(10)
        println(queue1.get())
        println("-------------------------------------------")

        // 叠加特质，越靠右越先起作用
        val queue2 = new BasicIntQueue with Incrementing with Filtering
        println(queue2)
        queue2.put(-1);
        queue2.put(0);
        queue2.put(1)
        // 1
        println(queue2.get())
        // 2
        println(queue2.get())

        var queue3 = new BasicIntQueue with Incrementing with Doubling
        println(queue3)
        queue3.put(48)
        println(queue3.get())
    }
}
