package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD3_Operator_MapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        // 【1，2】，【3，4】
        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if (index == 1) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        )

        mpiRDD.collect().foreach(println)
        sc.stop()
    }
}
