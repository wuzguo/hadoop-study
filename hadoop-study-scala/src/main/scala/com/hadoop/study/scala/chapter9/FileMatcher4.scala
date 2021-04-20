package com.hadoop.study.scala.chapter9

import java.io.File

object FileMatcher4 {

    private def filesHere = (new File(".")).listFiles

    def fileMatching(query: String, matcher: (String, String) => Boolean) =
        for (file <- filesHere; if matcher(file.getName, query))
            yield file

    def filesEnding(query: String) = fileMatching(query, _.endsWith(_))

    def filesContaining(query: String) = fileMatching(query, _.contains(_))

    def filesRegex(query: String) = fileMatching(query, _.matches(_))
}
