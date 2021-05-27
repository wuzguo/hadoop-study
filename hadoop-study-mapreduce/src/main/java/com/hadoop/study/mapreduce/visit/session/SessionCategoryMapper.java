package com.hadoop.study.mapreduce.visit.session;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 18:39
 */

@Slf4j
public class SessionCategoryMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    /**
     * 热门品类
     */
    private final List<Long> hotCategories = Lists.newArrayList();

    private final LongWritable key = new LongWritable();

    private final Text data = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        log.info("cache file name: {}", path);
        // 读取数据
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                // 2 切割
                String[] fields = line.split(":");
                // 3 缓存数据到集合
                hotCategories.add(Long.valueOf(fields[0].trim()));
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        String[] values = lines.split("_");

        List<Long> categories = Lists.newArrayList();

        // 计算点击品类ID
        if (!"-1".equals(values[6])) {
            categories.add(Long.valueOf(values[6]));
        }

        // 计算下单品类ID
        if (!"null".equals(values[8])) {
            // 品类集合
            categories.addAll(Arrays.stream(values[8].split(",")).map(Long::valueOf).collect(Collectors.toList()));
        }

        // 计算支付品类ID
        if (!"null".equals(values[10])) {
            // 品类集合
            categories.addAll(Arrays.stream(values[10].split(",")).map(Long::valueOf).collect(Collectors.toList()));
        }

        // 计算当前列是不是有热门品类
        for (Long category : categories.stream().filter(hotCategories::contains).collect(Collectors.toList())) {
            key.set(category);
            data.set(values[2]);
            context.write(key, data);
        }
    }
}
