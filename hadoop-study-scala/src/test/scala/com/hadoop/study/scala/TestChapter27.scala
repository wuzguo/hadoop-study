package com.hadoop.study.scala

import com.hadoop.study.scala.chapter27.{delayed, strategy}
import org.junit.{FixMethodOrder, Test}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/20 19:58
 */

@FixMethodOrder
class TestChapter27 {

    @Test
    def testAnnotation(): Unit = {

        @strategy(new delayed) def f() = {}

        println(f())

        @deprecated val f2 = () => println("hello")
        println(f2)

        @deprecated("use f4() instead")
        def f3(str: String): Unit = {
            println("Hello，" + str)
        }

        println(f3("Tomcat"))
    }
}
