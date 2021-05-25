package com.hadoop.study.spark.core.framework.common

import com.hadoop.study.spark.core.framework.util.ThreadUtil
import org.apache.spark.{SparkConf, SparkContext}

trait Application {

    def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
        val sparConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparConf)
        ThreadUtil.put(sc)

        try {
            op
        } catch {
            case ex => println(ex.getMessage)
        }

        // TODO 关闭连接
        sc.stop()
        ThreadUtil.clear()
    }
}
