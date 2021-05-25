package com.hadoop.study.spark.core.test

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/17 14:10
 */

class SubTask extends Serializable {

    var datas: List[Int] = _

    var logic: (Int) => Int = _

    // 计算
    def compute() = {
        datas.map(logic)
    }
}
