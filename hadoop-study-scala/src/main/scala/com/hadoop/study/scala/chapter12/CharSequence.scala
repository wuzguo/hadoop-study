package com.hadoop.study.scala.chapter12

trait CharSequence {

    def charAt(index: Int): Char

    def length: Int

    def subSequence(start: Int, end: Int): CharSequence

    def toString(): String
}
