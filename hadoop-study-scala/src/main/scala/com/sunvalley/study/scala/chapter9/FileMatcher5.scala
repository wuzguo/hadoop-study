package com.sunvalley.study.scala.chapter9

import java.io.File

object FileMatcher5 {

    private def filesHere = (new File(".")).listFiles

    def fileMatching(matcher: (String) => Boolean) =
        for (file <- filesHere; if matcher(file.getName))
            yield file

    def filesEnding(query: String) = fileMatching(_.endsWith(query))

    def filesContaining(query: String) = fileMatching(_.contains(query))

    def filesRegex(query: String) = fileMatching(_.matches(query))
}
