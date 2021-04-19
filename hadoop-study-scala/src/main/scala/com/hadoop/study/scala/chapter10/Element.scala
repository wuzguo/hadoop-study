package com.sunvalley.study.scala.chapter10

import com.sunvalley.study.scala.chapter10.factory.Element.elem

abstract class Element {

    def contents: Array[String]

    def width: Int = contents(0).length

    def height: Int = contents.length

    def demo(): Unit = println("Element's implementation invoked")

    def above(that: Element): Element = {
        val this1 = this widen that.width
        val that1 = that widen this.width
        elem(this1.contents ++ that1.contents)
    }

    def beside(that: Element): Element = {
        val this1 = this heighten that.height
        val that1 = that heighten this.height
        elem(
            for ((line1, line2) <- this1.contents zip that1.contents)
                yield line1 + line2)
    }

    def widen(w: Int): Element =
        if (w <= width) this
        else {
            val left = elem(' ', (w - width) / 2, height)
            val right = elem(' ', w - width - left.width, height)
            left beside this beside right
        }

    def heighten(h: Int): Element =
        if (h <= height) this
        else {
            val top = elem(' ', width, (h - height) / 2)
            val bot = elem(' ', width, h - height - top.height)
            top above this above bot
        }

    override def toString = contents mkString "\n"
}
