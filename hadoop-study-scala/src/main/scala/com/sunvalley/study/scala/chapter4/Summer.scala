package com.sunvalley.study.scala.chapter4

object Summer {

    def main(args: Array[String]): Unit = {
        for (arg <- args)
            println(arg + ": " + calculate(arg))
    }

}
