package com.hadoop.study.mapreduce.visit.conversion;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.PageAction;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

public class PageConversionMapper extends Mapper<LongWritable, Text, Text, PageAction> {

    private final Text session = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        String[] values = lines.split("_");

        session.set(values[2]);

        PageAction action = PageAction.builder().pageId(Integer.valueOf(values[3])).time(values[4]).build();
        context.write(session, action);
    }
}
