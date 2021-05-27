package com.hadoop.study.mapreduce.visit.top10;

import com.google.common.collect.Maps;
import com.hadoop.study.mapreduce.domain.HotCategory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 14:04
 */

@Slf4j
public class Top10CategoryMapper extends Mapper<LongWritable, Text, LongWritable, HotCategory> {

    private final LongWritable text = new LongWritable(0);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Map<Long, HotCategory> mapCategory = Maps.newConcurrentMap();

        String lines = value.toString();
        String[] values = lines.split("_");
        // 点击，下单，支付品类

        // 计算点击品类ID
        if (!"-1".equals(values[6])) {
            HotCategory category = HotCategory.builder().categoryId(Long.valueOf(values[6]))
                .clickCount(1).orderCount(0).payCount(0).build();
            mapCategory.put(Long.valueOf(values[6]), category);
        }

        // 计算下单品类ID
        if (!"null".equals(values[8])) {
            // 品类集合
            List<Long> categories = Arrays.stream(values[8].split(",")).map(Long::valueOf).collect(Collectors.toList());
            categories.forEach(category -> {
                HotCategory hotCategory = mapCategory.getOrDefault(category, HotCategory.builder().categoryId(category)
                    .clickCount(0).orderCount(0).payCount(0).build());
                hotCategory.setOrderCount(hotCategory.getOrderCount() + 1);
                mapCategory.put(category, hotCategory);
            });
        }

        // 计算支付品类ID
        if (!"null".equals(values[10])) {
            // 品类集合
            List<Long> categories = Arrays.stream(values[10].split(",")).map(Long::valueOf)
                .collect(Collectors.toList());
            categories.forEach(category -> {
                HotCategory hotCategory = mapCategory.getOrDefault(category, HotCategory.builder().categoryId(category)
                    .clickCount(0).orderCount(0).payCount(0).build());
                hotCategory.setPayCount(hotCategory.getPayCount() + 1);
                mapCategory.put(category, hotCategory);
            });
        }

        // 写出
        for (Entry<Long, HotCategory> entry : mapCategory.entrySet()) {
            text.set(entry.getKey());
            context.write(text, entry.getValue());
        }
    }
}