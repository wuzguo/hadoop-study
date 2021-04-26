package com.hadoop.study.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2020/7/3 15:48
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable intValue = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        // 1 累加求和
        int sum = 0;

        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        intValue.set(sum);
        context.write(key, intValue);
    }
}
