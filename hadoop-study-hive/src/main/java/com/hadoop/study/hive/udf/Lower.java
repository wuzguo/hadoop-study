package com.hadoop.study.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/6/29 15:49
 */

public class Lower extends UDF {

    // 计算方法
    public String evaluate(final String str) {
        return (str == null) ? null : str.toLowerCase();
    }
}
