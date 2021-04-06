package com.sunvalley.study.scala

import com.sunvalley.study.scala.ChecksumAccumulator.calculate

object Summer {

    def main(args: Array[String]): Unit = {
        for (arg <- args)
            println(arg + ": " + calculate(arg))
    }

}
