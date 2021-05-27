package com.hadoop.study.mapreduce.visit.session;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 18:39
 */

@Slf4j
public class SessionCategoryReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private final Text value = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        // SessionID
        List<String> sessions = Lists.newArrayList();
        for (Text value : values) {
            sessions.add(value.toString());
        }

        // SessionID 聚合
        TreeMap<String, Long> mapSessions = sessions.stream().reduce(new TreeMap<>(),
            (mapSession, sessionId) -> {
                Long sum = mapSession.getOrDefault(sessionId, 0L);
                mapSession.put(sessionId, ++sum);
                return mapSession;
            }, (mapSession, other) -> {
                other.forEach((sessionId, sum) -> {
                    Long total = mapSession.getOrDefault(sessionId, 0L);
                    total += sum;
                    mapSession.put(sessionId, total);
                });
                return mapSession;
            });

        //Map排序，这里将map.entrySet()转换成list
        List<Map.Entry<String, Long>> list = new ArrayList<>(mapSessions.entrySet());
        //然后通过比较器来实现排序
        //升序排序
        Collections.sort(list, Entry.comparingByValue());
        Collections.reverse(list);

        // 取前10名
        List<Entry<String, Long>> collect = list.stream().limit(10).collect(Collectors.toList());
        StringBuilder builder = new StringBuilder();
        for (Entry<String, Long> stringLongEntry : collect) {
            builder.append("(").append(stringLongEntry.getKey()).append(", ").append(stringLongEntry.getValue())
                .append("),");
        }

        value.set(builder.toString());
        context.write(key, value);
    }
}
