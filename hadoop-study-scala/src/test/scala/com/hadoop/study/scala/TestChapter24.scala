package com.hadoop.study.scala

import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 16:34
 */

@FixMethodOrder
class TestChapter24 {

    @Test
    def testMap(): Unit = {
        // List(2, 3, 4)
        println(List(1, 2, 3) map (_ + 1))

        // List(2, 3, 4)
        println(List(1, 2, 3).map(_ + 1))

        // List(1, 2, 3)
        val tr = Traversable(1, 2, 3)
        println(tr.toList)
    }

    @Test
    def testVector(): Unit = {
        val vec = scala.collection.immutable.Vector.empty
        println(vec)
        val vec2 = vec :+ 1 :+ 2
        println(vec2)
        val vec3 = 100 +: vec2
        println(vec3)

        val vec4 = Vector(1, 2, 3)
        val vec5 = vec4 updated(2, 4)
        println(vec5)
        println(vec4)
    }

    @Test
    def testStack(): Unit = {
        val stack = scala.collection.mutable.Stack[Int]()
        println(stack)
        val hasOne = stack.push(1)
        println(hasOne)

        hasOne.top
        println(hasOne)
    }

    @Test
    def testQueue(): Unit = {
        val empty = scala.collection.immutable.Queue[Int]()
        println(empty)
        val has1 = empty.enqueue(1)
        println(has1)
        val has123 = has1.enqueue(List(2, 3))
        println(has123)
        val (element, has23) = has123.dequeue
        println(element, has23)
    }

    @Test
    def testTreeSet(): Unit = {
        val set = collection.immutable.TreeSet.empty[Int]
        println(set + 1 + 3 + 3)
        println(set)

        val bits = scala.collection.immutable.BitSet.empty
        val moreBits = bits + 3 + 4 + 4
        println(moreBits(3))
        println(moreBits(0))
    }

    @Test
    def testArrayBuffer(): Unit = {
        val buf = collection.mutable.ArrayBuffer.empty[Int]
        println(buf)
        println(buf += 1)
        println(buf += 10)
        println(buf.toArray)
    }

    @Test
    def testListBuffer(): Unit = {
        val buf = collection.mutable.ListBuffer.empty[Int]
        println(buf += 1)
        println(buf += 10)
        println(buf.toList)
    }

    @Test
    def testStringBuffer(): Unit = {
        val buf = new StringBuilder
        println(buf += 'a')
        println(buf ++= "bcdef")
        println(buf.toString)
    }
}
