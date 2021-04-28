package com.hadoop.study.mapreduce.friends;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 15:25
 */

@Slf4j
public class TwoStepMapper extends Mapper<LongWritable, Text, Text, Text> {

    // 反向的问题
    private static final Map<Set<String>, String> mapPersonFriends = Maps.newConcurrentMap();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // A I,K,C,B,G,F,H,O,D,
        // 友 人，人，人
        String line = value.toString();
        String[] friendPersons = line.split("\t");

        String friend = friendPersons[0];
        String[] persons = friendPersons[1].split(",");
        Arrays.sort(persons);

        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i+ 1; j < persons.length - 1; j++) {
                // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
                Set<String> keys = Sets.newHashSet(persons[i].trim(), persons[j].trim());
                String person = String.join(" - ",  keys);
                context.write(new Text(person.trim()), new Text(friend.trim()));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Set<String>, String> entry : mapPersonFriends.entrySet()) {
            Set<String> keys = entry.getKey();
            String friend = entry.getValue();
            if (keys.size() == 2) {
                String person = String.join(" - ",  keys);
            //    context.write(new Text(person.trim()), new Text(friend.trim()));
            }
        }
    }
}
