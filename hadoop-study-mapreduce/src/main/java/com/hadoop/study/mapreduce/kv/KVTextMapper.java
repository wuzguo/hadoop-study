package com.hadoop.study.mapreduce.kv;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/26 17:24
 */

@Slf4j
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    // 1 设置value
    private final LongWritable values = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        log.info("{}, {}", key, value);
        // 2 写出
        context.write(key, values);
    }
}
