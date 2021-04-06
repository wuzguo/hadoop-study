package com.sunvalley.study.scala

import com.sunvalley.study.scala.ChecksumAccumulator.calculate

object FallWinterSpringSummer extends App {

    for (season <- List("fall", "winter", "spring"))
        println(season + ": " + calculate(season))
}
