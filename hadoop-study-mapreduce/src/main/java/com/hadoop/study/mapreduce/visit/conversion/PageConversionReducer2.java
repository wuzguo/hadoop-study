package com.hadoop.study.mapreduce.visit.conversion;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 19:35
 */

@Slf4j
public class PageConversionReducer2 extends Reducer<LongWritable, LongWritable, NullWritable, Text> {

    private final Text text = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        String value = String.format("%s:%s", key.get(), sum);
        text.set(value);
        context.write(NullWritable.get(), text);
    }
}
