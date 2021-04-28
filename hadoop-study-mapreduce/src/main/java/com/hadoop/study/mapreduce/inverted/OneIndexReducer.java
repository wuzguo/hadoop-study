package com.hadoop.study.mapreduce.inverted;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 11:42
 */

@Slf4j
public class OneIndexReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable sumValue = new LongWritable(0);

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        log.info("key: {}, sum: {}", key, sum);
        sumValue.set(sum);
        context.write(key, sumValue);
    }
}
