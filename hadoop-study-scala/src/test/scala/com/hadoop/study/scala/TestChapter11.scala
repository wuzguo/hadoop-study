package com.sunvalley.study.scala

import com.sunvalley.study.scala.chapter11.{Anchor, Dollars, Html, Style, SwissFrancs, Text}
import org.junit.{FixMethodOrder, Test}

@FixMethodOrder
class TestChapter11 {

    @Test
    def testHierarchy(): Unit = {
        println(42.toString)
        println(42.hashCode)
        println(42 equals 42)
        println(42 max 43)
        println(1 until 5)
        println(1 to 5)
        println(3.abs)
        println((-3).abs)
    }

    @Test
    def testImplemented(): Unit = {
        def isEqual(x: Int, y: Int) = x == y

        println(isEqual(421, 421))

        def isEqual2(x: Any, y: Any) = x == y

        println(isEqual2(421, 421))
    }

    @Test
    def testDefiningClass(): Unit = {
        val money = new Dollars(1000000)
        println(money)
        println(money.amount)

        val francs = new SwissFrancs(1000)
        println(francs)
    }

    @Test
    def testTitle(): Unit = {
        def title(text: Text, anchor: Anchor, style: Style): Html =
            new Html(s"<a id='${anchor.value}'><h1 class='${style.value}'>" + text.value + "</h1></a>")

        val html = title(new Text("Value Classes"), new Anchor("chap:vcls"), new Style("bold"))
        println(html.value)
    }
}
