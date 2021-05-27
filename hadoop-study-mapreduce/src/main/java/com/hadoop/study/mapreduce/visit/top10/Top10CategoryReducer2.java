package com.hadoop.study.mapreduce.visit.top10;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.HotCategory;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 14:04
 */

@Slf4j
public class Top10CategoryReducer2 extends Reducer<NullWritable, HotCategory, NullWritable, Text> {

    private final Text text = new Text();

    @Override
    protected void reduce(NullWritable key, Iterable<HotCategory> values, Context context)
        throws IOException, InterruptedException {

        List<HotCategory> categories = Lists.newLinkedList();
        for (HotCategory category : values) {
            HotCategory hotCategory = HotCategory.builder().categoryId(category.getCategoryId())
                .clickCount(category.getClickCount())
                .orderCount(category.getOrderCount())
                .payCount(category.getPayCount()).build();
            categories.add(hotCategory);
        }

        // 排序
        List<HotCategory> hotCategories = categories.stream().sorted(Comparator.reverseOrder()).limit(10)
            .collect(Collectors.toList());
        for (HotCategory category : hotCategories) {
            String value = String.format("%s : (%s, %s, %s)", category.getCategoryId(), category.getClickCount(),
                category.getOrderCount(), category.getPayCount());
            text.set(value);
            context.write(NullWritable.get(), text);
        }
    }
}