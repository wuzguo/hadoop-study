package com.sunvalley.study.scala

import org.junit.{FixMethodOrder, Test}

import java.io.File
import scala.io.Source

@FixMethodOrder
class TestChapter7 {

    @Test
    def testIf(): Unit = {
        val fileName = "default.txt"
        if (fileName.isEmpty) println("file nane is empty")
    }

    @Test
    def testFor(): Unit = {
        val filesHere = (new File(".")).listFiles()
        for (file <- filesHere) {
            println(file)
        }

        for (i <- 1 to 4)
            println("Iteration " + i)

        println("------------------------------")

        for (i <- 1 until 4)
            println("Iteration " + i)

        println("------------------------------")

        for (i <- 0 until filesHere.length)
            println(filesHere(i))
    }

    def fileLines(file: File) = {
        Source.fromFile(file).getLines().toList
    }

    def grep(pattern: String, filesHere: Array[File]) = {
        for (
            file <- filesHere
            if file.getName.endsWith(".scala");
            line <- fileLines(file)
            if line.trim.matches(pattern)
        ) println(file + ": " + line.trim)
    }

    @Test
    def testFilter(): Unit = {
        val filesHere = (new File(".\\src\\test\\scala\\com\\sunvalley\\study\\scala")).listFiles
        for (file <- filesHere
             if file.getName.endsWith(".scala"))
            println(file)

        println("------------------------------")

        for (file <- filesHere) {
            if (file.getName.endsWith(".scala"))
                println(file)
        }

        println("------------------------------")

        // 多个过滤器
        for (
            file <- filesHere
            if file.isFile
            if file.getName.endsWith(".scala")
        ) println(file)

        println("------------------------------")
        // 过滤器
        grep(".*println.*", filesHere)
    }


    @Test
    def testYield(): Unit = {
        val filesHere = (new File(".\\src\\test\\scala\\com\\sunvalley\\study\\scala")).listFiles

        def scalaFiles = for {
            file <- filesHere
            if file.getName.endsWith(".scala")
        } yield file

        scalaFiles.foreach(f => println(f))

        println("------------------------------")

        val forLineLengths = for {
            file <- filesHere
            if file.getName.endsWith(".scala")
            line <- fileLines(file)
            trimmed = line.trim
            if trimmed.matches(".*println.*")
        } yield trimmed.length

        forLineLengths.foreach(f => println(f))
    }

    @Test
    def testException(): Unit = {
        val n = 3
        //  val half = if (n % 2 == 0) n / 2 else throw new RuntimeException("n must be even")

        try {
            val half = if (n % 2 == 0) n / 2 else throw new RuntimeException("n must be even")
            println(half)
        } catch {
            case e: Throwable => println(s"错误信息 $e")
        } finally {
            println(" finished... ")
        }
    }

    @Test
    def testFinally(): Unit = {
        def f(): Int = try return 1 finally return 2
        // 2
        println(f())

        def g() = try 1 finally 2
        // 1
        println(g())
    }

    @Test
    def testMatch(): Unit = {
        val args: Array[String] = Array("salt")
        val firstArg = if (!args.isEmpty) args(0) else ""
        val friend = firstArg match {
            case "salt" => "pepper"
            case "chips" => "salsa"
            case "eggs" => "bacon"
            case _ => "huh?"
        }
        println(friend)
    }

    @Test
    def testBreak(): Unit = {
        val args: Array[String] = Array("Hello.scala")
        var i = 0
        var foundIt = false
        while (i < args.length && !foundIt) {
            if (!args(i).startsWith("-")) {
                if (args(i).endsWith(".scala")) {
                    println(args(i))
                    foundIt = true
                }
            }
            i = i + 1
        }
    }
}
