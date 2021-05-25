package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD7_Operator_Filter2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - filter
        val rdd = sc.textFile("./hadoop-study-datas/spark/data/apache.txt")

        rdd.filter(
            line => {
                val datas = line.split(" ")
                val time = datas(3)
                time.startsWith("17/05/2015")
            }
        ).collect().foreach(println)

        sc.stop()
    }
}
