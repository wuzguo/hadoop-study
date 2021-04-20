package com.hadoop.study.scala.chapter10.factory

import com.hadoop.study.scala.chapter10.Element

object Element2 {

    private class ArrayElement(
                                val contents: Array[String]
                              ) extends Element

    private class LineElement(s: String) extends Element {
        val contents = Array(s)

        override def width = s.length

        override def height = 1
    }

    private class UniformElement(
                                  ch: Char,
                                  override val width: Int,
                                  override val height: Int
                                ) extends Element {
        private val line = ch.toString * width

        def contents = Array.fill(height)(line)
    }

    def elem(contents: Array[String]): Element =
        new ArrayElement(contents)

    def elem(chr: Char, width: Int, height: Int): Element =
        new UniformElement(chr, width, height)

    def elem(line: String): Element =
        new LineElement(line)
}
