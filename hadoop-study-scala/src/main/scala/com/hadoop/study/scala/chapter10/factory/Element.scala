package com.hadoop.study.scala.chapter10.factory

import com.hadoop.study.scala.chapter10.{ArrayElement, Element, LineElement, UniformElement}

object Element {

    def elem(contents: Array[String]): Element =
        new ArrayElement(contents)

    def elem(chr: Char, width: Int, height: Int): Element =
        new UniformElement(chr, width, height)

    def elem(line: String): Element =
        new LineElement(line)
}
