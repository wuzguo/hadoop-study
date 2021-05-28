package com.hadoop.study.mapreduce.visit.conversion;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.PageAction;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 19:35
 */

@Slf4j
public class PageConversionReducer extends Reducer<Text, PageAction, NullWritable, Text> {

    /**
     * 拉链
     *
     * @param as  集合A
     * @param bs  集合B
     * @param <A> 泛型A
     * @param <B> 泛型B
     * @return {@link List< Pair >}
     */
    public static <A, B> List<Pair<A, B>> zip(List<A> as, List<B> bs) {
        return IntStream.range(0, Math.min(as.size(), bs.size()))
            .mapToObj(i -> new Pair<>(as.get(i), bs.get(i)))
            .collect(Collectors.toList());
    }

    private final Text value = new Text();

    public static void main(String[] args) {
        List<Pair<Integer, Integer>> zip = zip(Lists.newArrayList(1, 2, 3), Lists.newArrayList(2, 3));
        System.out.println(zip);
    }

    private List<String> mapPages = Lists.newArrayList();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        log.info("cache file name: {}", path);

        List<Integer> pages = Lists.newArrayList();
        // 读取数据
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                // 2 切割
                String[] fields = line.split(":");
                // 3 缓存数据到集合
                pages.add(Integer.valueOf(fields[0].trim()));
            }
        }

        List<Integer> next = pages.subList(1, pages.size());
        mapPages = zip(pages, next).stream().map(pair -> String.format("%s%s", pair.getFirst(), pair.getSecond()))
            .collect(Collectors.toList());
    }


    @Override
    protected void reduce(Text key, Iterable<PageAction> values, Context context)
        throws IOException, InterruptedException {

        List<PageAction> actions = Lists.newArrayList();
        for (PageAction value : values) {
            PageAction action = PageAction.builder().pageId(value.getPageId()).time(value.getTime()).build();
            actions.add(action);
        }
        // 排序
        Collections.sort(actions);
        // 获取页面ID
        List<Integer> pages = actions.stream().map(PageAction::getPageId).collect(Collectors.toList());

        // 拉链
        List<Integer> tails = pages.subList(1, pages.size());
        List<Pair<Integer, Integer>> zips = zip(pages, tails);
        // 写出字符串
        StringBuilder builder = new StringBuilder();
        for (Pair<Integer, Integer> zip : zips) {
            String zipStr = String.format("%s%s",zip.getFirst(), zip.getSecond());
            if (mapPages.contains(zipStr)) {
                builder.append(zip.getFirst()).append("->").append(zip.getSecond()).append(",");
            }
        }

        // 如果不为空
        if (StringUtils.isNotEmpty(builder.toString())) {
            value.set(builder.toString());
            context.write(NullWritable.get(), value);
        }
    }
}
