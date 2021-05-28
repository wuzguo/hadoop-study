package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:59
 */

object SparkSQL_UDAF {

    def main(args: Array[String]): Unit = {

        // 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDAF")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")
        df.createOrReplaceTempView("user")

        // 2. 注册聚合函数
        spark.udf.register("avg", new AvgAggFunction())

        // 3. 使用聚合函数
        spark.sql("select avg(age) as avgValue from user").show


        // 4. 关闭环境
        spark.close()
    }

    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承 UserDefinedAggregateFunction
     2. 重写方法(8)
     */
    class AvgAggFunction extends UserDefinedAggregateFunction {
        // 输入数据类型
        override def inputSchema: StructType = StructType {
            // 名称跟字段名称无关
            Array(StructField("age", LongType))
        }

        // 缓冲区类型
        override def bufferSchema: StructType = StructType {
            StructType(
                Array(
                    StructField("total", LongType),
                    StructField("count", LongType)
                )
            )
        }

        // 函数计算结果的数据类型：Out
        override def dataType: DataType = DoubleType

        // 函数的稳定性
        override def deterministic: Boolean = true

        // 初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0, 0L)
            buffer.update(1, 0L)
        }

        // 更新
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
        }

        // 合并
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }

        // 计算平均值
        override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}
