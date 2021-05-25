package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD1_Operator_Map3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        // 【1，2】，【3，4】
        rdd.saveAsTextFile("./hadoop-study-datas/spark/output")
        val mapRDD = rdd.map(_ * 2)
        // 【2，4】，【6，8】
        mapRDD.saveAsTextFile("output1")

        sc.stop()

    }
}
