package com.hadoop.study.mapreduce.friends;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 15:29
 */

@Slf4j
public class TwoStepReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> friends = Lists.newLinkedList();
        for (Text value : values) {
            friends.add(value.toString());
        }
        String friend = friends.stream().sorted(String::compareTo).collect(Collectors.joining(" , "));
        Text text = new Text(String.format("( %s )", friend.trim()));
        // 人-人 友，友，友
        context.write(key, text);
    }
}
