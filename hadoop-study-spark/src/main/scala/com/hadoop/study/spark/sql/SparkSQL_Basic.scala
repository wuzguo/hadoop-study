package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:07
 */

object SparkSQL_Basic {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Basic")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")

        // 1. 创建全局视图
        df.createGlobalTempView("user")
        spark.sql("select * from global_temp.user").show()

        // 2. 创建Session的视图
        df.createOrReplaceTempView("user")
        spark.sql("select * from user").show()

        // 3. 添加函數
        spark.sql("select avg(age) from user").show()

        // 4. DSL指定列
        df.select("name", "age").show()

        // 5. 結果运算
        import spark.implicits._
        df.select('age + 1).show()
        df.select($"age" + 1).show()

        // 6. DataSet
        val seq = Seq(1, 2, 3, 4)
        val ds: Dataset[Int] = seq.toDS()
        ds.show()

        // 7. RDD <=> DataFrame
        val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
        val df2 = rdd.toDF("id", "name", "age")
        val rowRDD: RDD[Row] = df2.rdd
        // [1,zhangsan,30]
        // [2,lisi,40]
        rowRDD.collect().foreach(println)


        // 8. RDD <=> DataSet
        val ds1: Dataset[User] = rdd.map {
            case (id, name, age) =>
                User(id, name, age)
        }.toDS()
        val userRDD: RDD[User] = ds1.rdd
        // User(1,zhangsan,30)
        // User(2,lisi,40)
        userRDD.collect().foreach(println)

        spark.stop()
    }


    case class User(id: Int, name: String, age: Int)
}
