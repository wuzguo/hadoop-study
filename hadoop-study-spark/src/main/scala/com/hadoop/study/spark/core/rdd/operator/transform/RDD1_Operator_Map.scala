package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD1_Operator_Map {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        // 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 0)
        // 1,2,3,4
        // 2,4,6,8

        // 转换函数
        def mapFunction(num: Int): Int = {
            num * 2
        }

        val mapRDD: RDD[Int] = rdd.map(mapFunction)
        mapRDD.saveAsTextFile("./hadoop-study-datas/spark/output")
        sc.stop()
    }
}
