package com.hadoop.study.scala.chapter4

import com.hadoop.study.scala.chapter4.ChecksumAccumulator.calculate

object FallWinterSpringSummer extends App {

    for (season <- List("fall", "winter", "spring"))
        println(season + ": " + calculate(season))
}
