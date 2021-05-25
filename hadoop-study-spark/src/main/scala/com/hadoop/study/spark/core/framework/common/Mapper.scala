package com.hadoop.study.spark.core.framework.common

import com.hadoop.study.spark.core.framework.util.ThreadUtil

trait Mapper {

    def readFile(path: String) = {
        ThreadUtil.take().textFile(path)
    }
}
