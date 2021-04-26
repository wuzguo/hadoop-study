package com.hadoop.study.mapreduce.kv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/26 17:25
 */

public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable value = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {
        long sum = 0L;

        // 1 汇总统计
        for (LongWritable value : values) {
            sum += value.get();
        }

        value.set(sum);
        // 2 输出
        context.write(key, value);
    }
}
