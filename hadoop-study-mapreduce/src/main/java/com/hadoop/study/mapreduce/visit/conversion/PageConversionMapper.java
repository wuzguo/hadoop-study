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

    /**
     * 拉链
     *
     * @param as  集合A
     * @param bs  集合B
     * @param <A> 泛型A
     * @param <B> 泛型B
     * @return {@link List<Pair>}
     */
    public static <A, B> List<Pair<A, B>> zip(List<A> as, List<B> bs) {
        return IntStream.range(0, Math.min(as.size(), bs.size()))
            .mapToObj(i -> new Pair<>(as.get(i), bs.get(i)))
            .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        List<Pair<Integer, Integer>> zip = zip(Lists.newArrayList(1, 2, 3), Lists.newArrayList(2, 3));
        System.out.println(zip);
    }

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
