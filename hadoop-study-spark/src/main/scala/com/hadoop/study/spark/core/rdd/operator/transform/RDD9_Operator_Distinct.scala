package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD9_Operator_Distinct {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - distinct
        // 将数据集中重复的数据去重
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

        // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

        // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
        // (1, null)(1, null)(1, null)
        // (null, null) => null
        // (1, null) => 1
        val rdd1: RDD[Int] = rdd.distinct()

        rdd1.collect().foreach(println)
        sc.stop()
    }
}
