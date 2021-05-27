package com.hadoop.study.mapreduce.visit.conversion;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.PageAction;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Pair;
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
        // 倒序
        Collections.reverse(actions);
        // 获取页面ID
        List<Integer> pages = actions.stream().map(PageAction::getPageId).collect(Collectors.toList());

        List<Integer> tails = pages.subList(1, pages.size());

        List<Pair<Integer, Integer>> zips = zip(pages, tails);

    }
}
