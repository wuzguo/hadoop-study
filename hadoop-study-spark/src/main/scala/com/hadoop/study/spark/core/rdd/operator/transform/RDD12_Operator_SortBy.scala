package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD12_Operator_SortBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy
        val rdd = sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2)

        val newRDD: RDD[Int] = rdd.sortBy(num => num)

        newRDD.saveAsTextFile("./hadoop-study-datas/spark/output")
        sc.stop()
    }
}
