package com.hadoop.study.scala.chapter3

object FindLongLines {

    def main(args: Array[String]) = {
        val width = args(0).toInt
        for (arg <- args.drop(1))
            LongLines.processFile(arg, width)
    }
}
