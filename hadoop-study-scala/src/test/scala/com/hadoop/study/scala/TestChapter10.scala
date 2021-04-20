package com.hadoop.study.scala

import com.hadoop.study.scala.chapter10._
import com.hadoop.study.scala.chapter10.factory.Spiral
import org.junit.{FixMethodOrder, Test}

@FixMethodOrder
class TestChapter10 {

    @Test
    def testArrayElement(): Unit = {
        val element = new ArrayElement(Array("Hello", "Tomcat"))
        // contents 是方法
        element.contents.foreach(println)
        println(element.height)
        println("--------------------------------------------")

        val element2 = new ArrayElement2(Array("Hello", "Tomcat"))
        // contents 是属性字段
        element2.contents.foreach(println)
        println(element2.height)
        println("--------------------------------------------")

        val element3 = new ArrayElement3(Array("Hello", "Tomcat"))
        // contents 是属性字段
        element3.contents.foreach(println)
        println(element3.height)
        println("--------------------------------------------")
        element3.contents = Array("Hello", "Jack")
        element3.contents.foreach(println)
        println("--------------------------------------------")
    }

    @Test
    def testLineElement() {
        val line = new LineElement("Jetty")
        // contents 方法
        line.contents.foreach(println)
        println(line.height)
        println("--------------------------------------------")
    }

    @Test
    def testUniformElement(): Unit = {
        val uniform = new UniformElement('6', 10, 20)
        // contents 方法
        uniform.contents.foreach(println)
        println(uniform.height)
        println("--------------------------------------------")
    }

    @Test
    def testInvokeDemo(): Unit = {

        def invokeDemo(e: Element) = e.demo()

        invokeDemo(new ArrayElement(Array("Hello")))
        invokeDemo(new LineElement("Hello"))
        invokeDemo(new UniformElement('6', 10, 20))
    }

    @Test
    def testSpiral(): Unit = {
        Spiral.main(Array("6"))
        Spiral.main(Array("16"))
        Spiral.main(Array("36"))
        Spiral.main(Array("136"))
    }
}
