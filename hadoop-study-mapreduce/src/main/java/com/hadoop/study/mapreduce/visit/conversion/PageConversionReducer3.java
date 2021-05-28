package com.hadoop.study.mapreduce.visit.conversion;

import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 19:35
 */

@Slf4j
public class PageConversionReducer3 extends Reducer<Text, LongWritable, NullWritable, Text> {

    private final Map<Integer, Integer> mapPageSum = Maps.newHashMap();

    private final Text text = new Text();

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
                mapPageSum.put(Integer.valueOf(fields[0].trim()), Integer.valueOf(fields[1].trim()));
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }

        String pages = key.toString();
        int page = Integer.parseInt(pages.trim().split("->")[0]);

        double rate = (sum * 1.0) / mapPageSum.get(page);
        String value = String.format("页面 %s 转化率： %s，访问数量： %s, %s", key, rate, sum, mapPageSum.get(page));
        text.set(value);
        context.write(NullWritable.get(), text);
    }

}
