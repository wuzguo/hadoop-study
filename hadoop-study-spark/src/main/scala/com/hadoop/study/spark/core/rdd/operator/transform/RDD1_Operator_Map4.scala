package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD1_Operator_Map4 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        val rdd = sc.textFile("./hadoop-study-datas/spark/data/apache.txt")

        // 长的字符串
        // 短的字符串
        val mapRDD: RDD[String] = rdd.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )
        mapRDD.collect().foreach(println)

        sc.stop()

    }
}
