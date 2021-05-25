package com.hadoop.study.spark.core.framework.util

import org.apache.spark.SparkContext

object ThreadUtil {

    private val threadLocal = new ThreadLocal[SparkContext]()

    def put(sc: SparkContext): Unit = {
        threadLocal.set(sc)
    }

    def take(): SparkContext = {
        threadLocal.get()
    }

    def clear(): Unit = {
        threadLocal.remove()
    }
}
