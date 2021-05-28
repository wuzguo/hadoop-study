package com.hadoop.study.mapreduce.visit.conversion;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 19:35
 */

@Slf4j
public class PageConversionMapper3 extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text page = new Text();

    private final LongWritable sum = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        // 为空直接返回
        if (StringUtils.isEmpty(lines)) {
            return;
        }

        String[] values = lines.split(",");
        for (String str : values) {
            page.set(str);
            context.write(page, sum);
        }
    }
}
