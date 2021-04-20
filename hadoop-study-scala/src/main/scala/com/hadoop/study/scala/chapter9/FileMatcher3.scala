package com.hadoop.study.scala.chapter9

import java.io.File

object FileMatcher3 {

    private def filesHere = (new File(".")).listFiles

    def fileMatching(query: String, matcher: (String, String) => Boolean) =
        for (file <- filesHere; if matcher(file.getName, query))
            yield file

    def filesEnding(query: String) = fileMatching(query, (fileName, query) => fileName.endsWith(query))

    def filesContaining(query: String) = fileMatching(query, (fileName, query) => fileName.contains(query))

    def filesRegex(query: String) = fileMatching(query, (fileName, query) => fileName.matches(query))
}
