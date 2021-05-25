package com.hadoop.study.spark.core.framework.service

import com.hadoop.study.spark.core.framework.common.Service
import com.hadoop.study.spark.core.framework.mapper.WordCountMapper
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService extends Service {

    private val wordCountDao = new WordCountMapper()

    // 数据分析
    def dataAnalysis() = {

        val lines = wordCountDao.readFile("datas/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => (word, 1))
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        val array: Array[(String, Int)] = wordToSum.collect()
        array
    }
}
