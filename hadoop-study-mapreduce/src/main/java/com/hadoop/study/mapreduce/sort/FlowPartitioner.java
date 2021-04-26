package com.hadoop.study.mapreduce.sort;

import com.hadoop.study.mapreduce.domain.FlowBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/26 17:06
 */

@Slf4j
public class FlowPartitioner extends Partitioner<FlowBean, Text> {

    // 全部分布在第一个文件中
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        log.info("{}, {}, {}", text, flowBean, i);
        return (text.hashCode() & Integer.MAX_VALUE) % i;
    }
}
