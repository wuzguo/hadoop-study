package com.hadoop.study.mapreduce.inverted;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 13:56
 */

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value: values) {
            builder.append(value.toString()).append(" ");
        }
        text.set(builder.toString());
        context.write(key, text);
    }
}
