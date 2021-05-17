package com.hadoop.study.spark.core.wc

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/17 14:10
 */

object WordCount {

    def main(args: Array[String]): Unit = {
        // 创建SparkConf并设置APP名称
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        // 创建SparkContext，该对象是提交Spark APP的入口
        val sc = new SparkContext(conf)
        //3.使用sc创建RDD并执行相应的transform和action
        sc.textFile("hdfs://hadoop001:9000/user/input/1.txt").flatMap(_.split(" "))
          .filter(str => StringUtils.isNotBlank(str))
          .map((_, 1))
          .reduceByKey(_ + _, 1)
          .sortBy(_._2, false)
          .saveAsTextFile("hdfs://hadoop001:9000/user/output3")
        //4.关闭连接
        sc.stop()
    }
}
