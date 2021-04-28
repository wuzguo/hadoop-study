package com.hadoop.study.mapreduce.inverted;

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
 * @date 2021/4/28 13:51
 */

@Slf4j
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text text = new Text();

    // 输出的值
    private Text valueText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        log.info("key: {}, value: {}, fields: {}", key, value, fields);
        text.set(fields[0]);
        valueText.set(String.format("%s --> %s", fields[1], fields[2]));
        context.write(text, valueText);
    }
}
