package com.hadoop.study.scala.chapter4

import com.hadoop.study.scala.chapter4.ChecksumAccumulator.calculate

object Summer {

    def main(args: Array[String]): Unit = {
        for (arg <- args)
            println(arg + ": " + calculate(arg))
    }

}
