package com.hadoop.study.scala.chapter10

class LineElement(str: String) extends ArrayElement(Array(str)) {

    override def width: Int = str.length

    override def height: Int = 1

    override def demo(): Unit = println("LineElement's implementation invoked")
}
