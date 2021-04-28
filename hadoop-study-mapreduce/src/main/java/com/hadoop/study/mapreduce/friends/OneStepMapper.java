package com.hadoop.study.mapreduce.friends;

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
 * @date 2021/4/28 15:15
 */

@Slf4j
public class OneStepMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行 A:B,C,D,F,E,O
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(":");

        // 3 获取person和好友
        String person = fields[0];
        String[] friends = fields[1].split(",");

        // 4写出去
        for (String friend : friends) {
            // 输出 <好友，人>
            context.write(new Text(friend.trim()), new Text(person.trim()));
        }
    }
}
