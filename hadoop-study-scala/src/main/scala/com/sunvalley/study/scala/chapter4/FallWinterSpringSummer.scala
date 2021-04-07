package com.sunvalley.study.scala.chapter4

object FallWinterSpringSummer extends App {

    for (season <- List("fall", "winter", "spring"))
        println(season + ": " + calculate(season))
}
