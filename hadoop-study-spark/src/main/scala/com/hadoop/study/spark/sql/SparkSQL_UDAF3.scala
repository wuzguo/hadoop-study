package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:59
 */

object SparkSQL_UDAF3 {

    def main(args: Array[String]): Unit = {

        // 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDAF3")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")
        df.createOrReplaceTempView("user")

        // 早期版本中，spark不能在sql中使用强类型UDAF操作
        // SQL & DSL
        // 早期的UDAF强类型聚合函数使用DSL语法操作
        import spark.implicits._
        val ds: Dataset[User] = df.as[User]

        // 将UDAF函数转换为查询的列对象
        val udafCol: TypedColumn[User, Double] = new AvgAggFunction().toColumn

        // 查询 DSL
        ds.select(udafCol).show

        // 4. 关闭环境
        spark.close()
    }

    /**
     * 用户对象
     *
     * @param name  名称
     * @param age   年龄
     * @param email 邮箱
     */
    case class User(name: String, age: Long, email: String)

    case class Buffer(var total: Long, var count: Long)

    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承 UserDefinedAggregateFunction
     2. 重写方法(8)
     */
    class AvgAggFunction extends Aggregator[User, Buffer, Double] {
        // z & zero : 初始值或零值
        // 缓冲区的初始化
        override def zero: Buffer = Buffer(0L, 0L)

        // 根据输入的数据更新缓冲区的数据
        override def reduce(buf: Buffer, user: User): Buffer = {
            buf.total += user.age
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
