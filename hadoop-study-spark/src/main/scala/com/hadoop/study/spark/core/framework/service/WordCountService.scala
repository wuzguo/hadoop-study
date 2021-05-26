package com.hadoop.study.spark.core.framework.service

import com.hadoop.study.spark.core.framework.common.Service
import com.hadoop.study.spark.core.framework.mapper.WordCountMapper
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService extends Service {

    private val wcMapper = new WordCountMapper()

    // 数据分析
    def dataAnalysis(): Array[(String, Int)] = {
        val lines = wcMapper.readFile("./hadoop-study-datas/spark/data/1.txt")
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => (word, 1))
        val wordToSum = wordToOne.reduceByKey(_ + _)
        wordToSum.collect()
    }
}
