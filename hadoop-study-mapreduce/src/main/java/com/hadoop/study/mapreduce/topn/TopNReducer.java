package com.hadoop.study.mapreduce.topn;

import com.hadoop.study.mapreduce.domain.FlowBean2;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 14:29
 */

public class TopNReducer extends Reducer<FlowBean2, Text, Text, NullWritable> {

    // 天然按key排序
    private final AtomicInteger increase = new AtomicInteger(0);

    // 值
    private final Text text = new Text();

    @Override
    protected void reduce(FlowBean2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if (increase.incrementAndGet() < 10) {
                String filed = String.format("%s\t%s", value, key);
                text.set(filed);
                context.write(text, NullWritable.get());
            }
        }
    }
}
