package com.hadoop.study.spark.core.framework.application

import com.hadoop.study.spark.core.framework.common.Application
import com.hadoop.study.spark.core.framework.controller.WordCountController

object WordCountApplication extends App with Application {

    // 启动应用程序
    start() {
        val controller = new WordCountController()
        controller.dispatch()
    }

}
