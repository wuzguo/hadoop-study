package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD4_Operator_FlatMap3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

        val flatRDD = rdd.flatMap {
            case list: List[_] => list
            case value => List(value)
        }

        flatRDD.collect().foreach(println)
        sc.stop()
    }
}
