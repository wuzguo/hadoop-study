package com.hadoop.study.scala.batch.wc

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/7 11:10
 */

object WordCount {

    def main(args: Array[String]): Unit = {
        // 1. 获取执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        // 2. 读取文件
        val lines: DataSet[String] = env.readTextFile("./hadoop-study-datas/flink/core/1.txt")

        // 3. 集合
        val ds: DataSet[(String, Int)] = lines.flatMap(new FlatMapper).groupBy(0).sum(1)

        // 4. 打印
        ds.print()
    }

    class FlatMapper extends FlatMapFunction[String, (String, Int)] {

        override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
            // 按空格分词
            val words = value.split(" ")
            // 遍历所有word，包成二元组输出
            words.foreach(word => out.collect((word, 1)))
        }
    }
}
