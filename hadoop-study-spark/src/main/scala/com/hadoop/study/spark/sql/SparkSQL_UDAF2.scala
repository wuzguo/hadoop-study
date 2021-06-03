package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:59
 */

object SparkSQL_UDAF2 {

    def main(args: Array[String]): Unit = {

        // 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDAF2")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")
        df.createOrReplaceTempView("user")

        // 2. 注册聚合函数
        spark.udf.register("avg", functions.udaf(new AvgAggFunction))

        // 3. 使用聚合函数
        spark.sql("select avg(age) as avgValue from user").show

        // 4. 关闭环境
        spark.close()
    }

    case class Buffer(var total: Long, var count: Long)

    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承 UserDefinedAggregateFunction
     2. 重写方法(8)
     */
    class AvgAggFunction extends Aggregator[Long, Buffer, Double] {
        // z & zero : 初始值或零值
        // 缓冲区的初始化
        override def zero: Buffer = Buffer(0L, 0L)

        // 根据输入的数据更新缓冲区的数据
        override def reduce(buf: Buffer, age: Long): Buffer = {
            buf.total += age
            buf.count += 1
            buf
        }

        // 合并缓冲区
        override def merge(pre: Buffer, next: Buffer): Buffer = {
            pre.total += next.total
            pre.count += next.count
            pre
        }

        //计算结果
        override def finish(reduction: Buffer): Double = {
            reduction.total.toDouble / reduction.count
        }

        // 缓冲区的编码操作
        override def bufferEncoder: Encoder[Buffer] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }
}
