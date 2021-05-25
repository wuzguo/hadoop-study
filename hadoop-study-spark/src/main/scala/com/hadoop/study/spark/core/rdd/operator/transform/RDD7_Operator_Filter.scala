package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD7_Operator_Filter {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - filter
        // 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)

        filterRDD.collect().foreach(println)
        sc.stop()
    }
}
