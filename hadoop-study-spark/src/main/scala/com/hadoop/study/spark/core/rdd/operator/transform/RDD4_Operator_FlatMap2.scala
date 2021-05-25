package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD4_Operator_FlatMap2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        // 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
        val rdd: RDD[String] = sc.makeRDD(List(
            "Hello Scala", "Hello Spark"
        ))

        val flatRDD: RDD[String] = rdd.flatMap(
            s => {
                s.split(" ")
            }
        )

        flatRDD.collect().foreach(println)
        sc.stop()
    }
}
