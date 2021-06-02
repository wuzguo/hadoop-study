package com.hadoop.study.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Operator_Action3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List(1,1,1,4),2)
        val rdd = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2,"b"), (3, "c"), (3, "c")))

        // TODO - 行动算子

        //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        //println(intToLong)
        val stringToLong = rdd.countByKey()

        println(stringToLong)
        sc.stop()
    }
}
