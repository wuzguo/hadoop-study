package com.sunvalley.study.scala.chapter9

import java.io.File

object FileMatcher2 {

    private def filesHere = (new File(".")).listFiles

    def fileMatching(query: String, matcher: (String, String) => Boolean) =
        for (file <- filesHere; if matcher(file.getName, query))
            yield file

    def filesEnding(query: String) = fileMatching(query, (fileName: String, query: String) => fileName.endsWith(query))

    def filesContaining(query: String) = fileMatching(query, (fileName: String, query: String) => fileName.contains(query))

    def filesRegex(query: String) = fileMatching(query, (fileName: String, query: String) => fileName.matches(query))
}
