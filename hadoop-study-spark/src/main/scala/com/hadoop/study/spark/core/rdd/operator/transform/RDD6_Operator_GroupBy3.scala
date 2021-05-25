package com.hadoop.study.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object RDD6_Operator_GroupBy3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd = sc.textFile("./hadoop-study-datas/spark/data/apache.txt")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
            line => {
                val datas = line.split(" ")
                val time = datas(3)
                //time.substring(0, )
                val dataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = dataFormat.parse(time)
                val hourDataFormat = new SimpleDateFormat("HH")
                val hour = hourDataFormat.format(date)
                (hour, 1)
            }
        ).groupBy(_._1)

        timeRDD.sortBy(_._1, ascending = true).map {
            case (hour, iter) => (hour, iter.size)
        }.collect.foreach(println)

        sc.stop()
    }
}
