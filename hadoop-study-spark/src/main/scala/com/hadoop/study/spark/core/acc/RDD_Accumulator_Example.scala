package com.hadoop.study.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 15:50
 */

object RDD_Accumulator_Example {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Accumulator_WD")
        val sc = new SparkContext(conf)
        val mapRDD = sc.makeRDD(List("hello", "world", "tomcat", "spark", "rdd", "tomcat", "spark", "tomcat", "rdd", "rdd", "rdd", "rdd", "rdd", "spark"))

        // 向Spark进行注册
        val accumulator = new CustomAccumulatorV2
        sc.register(accumulator, "accumulator")

        // 数据的累加（使用累加器）
        mapRDD.foreach(num => accumulator.add(num))
        println(accumulator.value)
        sc.stop()
    }

    /**
     * 自定义累加器
     */
    class CustomAccumulatorV2 extends AccumulatorV2[String, mutable.Map[String, Long]] {
        // 计数
        private val wcMap = mutable.Map[String, Long]()

        override def isZero: Boolean = wcMap.isEmpty

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new CustomAccumulatorV2

        override def reset(): Unit = wcMap.clear()

        override def add(v: String): Unit = {
            val value = wcMap.getOrElse(v, 0L) + 1
            wcMap.update(v, value)
        }

        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val values = other.value

            values.foreach {
                case (key, value) =>
                    val newCount = wcMap.getOrElse(key, 0L) + value
                    wcMap.update(key, newCount)
                case _ =>
            }
        }

        override def value: mutable.Map[String, Long] = wcMap
    }
}
