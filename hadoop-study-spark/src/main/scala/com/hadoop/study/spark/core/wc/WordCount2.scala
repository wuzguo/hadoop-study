package com.hadoop.study.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/25 11:19
 */

object WordCount2 {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        wordCount0(sc)
        sc.stop()
    }

    // reduce, aggregate, fold
    def wordCount0(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))

        // 【（word, count）,(word, count)】
        // word => Map[(word,1)]
        val mapWord = words.map(
            word => {
                mutable.Map[String, Long]((word, 1))
            }
        )

        val wordCount = mapWord.reduce(
            (map1, map2) => {
                map2.foreach {
                    case (word, count) =>
                        val newCount = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                }
                map1
            }
        )

        println(wordCount)
    }

    // groupBy
    def wordCount1(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
        println(wordCount)
    }

    // groupByKey
    def wordCount2(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
        println(wordCount)
    }

    // reduceByKey
    def wordCount3(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
        println(wordCount)
    }

    // aggregateByKey
    def wordCount4(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
        println(wordCount)
    }

    // foldByKey
    def wordCount5(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
        println(wordCount)
    }

    // combineByKey
    def wordCount6(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
            v => v,
            (x: Int, y) => x + y,
            (x: Int, y: Int) => x + y
        )
        println(wordCount)
    }

    // countByKey
    def wordCount7(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount: collection.Map[String, Long] = wordOne.countByKey()
        println(wordCount)
    }

    // countByValue
    def wordCount8(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = words.countByValue()
        println(wordCount)
    }
}
