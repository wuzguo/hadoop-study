package com.hadoop.study.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RDD_Accumulator4 {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 3)
        ))
        val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

        // 封装广播变量
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

        // 方法广播变量
        rdd1.map {
            case (w, c) =>
                val l: Int = bc.value.getOrElse(w, 0)
                (w, (c, l))
        }.collect().foreach(println)

        sc.stop()
    }
}
