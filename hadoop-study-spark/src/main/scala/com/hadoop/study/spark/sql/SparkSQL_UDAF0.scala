package com.hadoop.study.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/28 14:48
 */

object SparkSQL_UDAF0 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_UDF")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        // 读取文件
        val df = spark.read.json("./hadoop-study-datas/spark/sql/user.json")

        // 2. 创建Session的视图
        df.createOrReplaceTempView("user")

        // 3. 添加UDF函数
        spark.udf.register("prefix", (name: String) => {
            "name: " + name
        })

        // 4. 查询
        spark.sql("select age, prefix(name) from user").show

        // 5. 关闭
        spark.stop()
    }
}
