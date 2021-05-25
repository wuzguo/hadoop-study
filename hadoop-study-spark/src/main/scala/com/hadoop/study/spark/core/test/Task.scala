package com.hadoop.study.spark.core.test

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/17 14:10
 */

class Task extends Serializable {

    val datas = List(1, 2, 3, 4)

    //val logic = ( num:Int )=>{ num * 2 }
    val logic: (Int) => Int = _ * 2
}
