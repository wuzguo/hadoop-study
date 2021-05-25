package com.hadoop.study.spark.core.framework.controller

import com.hadoop.study.spark.core.framework.common.Controller
import com.hadoop.study.spark.core.framework.service.WordCountService


/**
 * 控制层
 */
class WordCountController extends Controller {

    private val wordCountService = new WordCountService()

    // 调度
    def dispatch(): Unit = {
        // TODO 执行业务操作
        val array = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}
