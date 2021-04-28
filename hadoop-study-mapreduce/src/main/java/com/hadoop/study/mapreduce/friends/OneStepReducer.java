package com.hadoop.study.mapreduce.friends;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 15:17
 */

@Slf4j
public class OneStepReducer extends Reducer<Text, Text, Text, Text> {

    private final Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString()).append(", ");
        }
        String person = builder.substring(0, builder.lastIndexOf(","));
        text.set(person.trim());
        // 友 人， 人， 人
        context.write(key, text);
    }
}
