package com.hadoop.study.mapreduce.agent;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/26 9:14
 */

public class OneAgentMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text text = new Text();

    private final Text city = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 2 切割
        String[] fields = line.split(" ");
        text.set(fields[1]);
        city.set(fields[4]);
        context.write(text, city);
    }
}
