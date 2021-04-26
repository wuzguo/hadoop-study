package com.hadoop.study.mapreduce.combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2020/7/3 15:39
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text text = new Text();

    private final IntWritable intValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            text.set(word);
            context.write(text, intValue);
        }
    }
}
