package com.hadoop.study.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Accumulator {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

        // reduce : 分区内计算，分区间计算
        //val i: Int = rdd.reduce(_+_)
        //println(i)
        var sum = 0
        rdd.foreach(
            num => {
                sum += num
            }
        )
        println("sum = " + sum)

        sc.stop()
    }
}
