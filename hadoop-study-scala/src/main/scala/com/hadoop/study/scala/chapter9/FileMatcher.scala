package com.sunvalley.study.scala.chapter9

import java.io.File

object FileMatcher {

    private def filesHere = (new File(".")).listFiles

    def filesEnding(query: String) =
        for (file <- filesHere; if file.getName.endsWith(query))
            yield file

    def filesContaining(query: String) =
        for (file <- filesHere; if file.getName.contains(query))
            yield file

    def filesRegex(query: String) =
        for (file <- filesHere; if file.getName.matches(query))
            yield file
}
