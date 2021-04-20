package com.hadoop.study.scala.chapter2

class MyClass(index: Int, name: String) {

    override def toString: String = {
        index + ", " + name
    }
}
