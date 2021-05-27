package com.hadoop.study.mapreduce.visit.top10;

import com.google.common.collect.Lists;
import com.hadoop.study.mapreduce.domain.HotCategory;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 14:04
 */

@Slf4j
public class Top10CategoryReducer extends Reducer<LongWritable, HotCategory, NullWritable, HotCategory> {

    @Override
    protected void reduce(LongWritable key, Iterable<HotCategory> values, Context context)
        throws IOException, InterruptedException {

        // 这里一定要 new 新对象，不然结果不正确
        List<HotCategory> categories = Lists.newLinkedList();
        for (HotCategory category : values) {
            HotCategory hotCategory = HotCategory.builder().categoryId(category.getCategoryId())
                .clickCount(category.getClickCount())
                .orderCount(category.getOrderCount())
                .payCount(category.getPayCount()).build();
            categories.add(hotCategory);
        }

        // 合并计算
        HotCategory zeroCategory = HotCategory.builder()
            .categoryId(key.get()).clickCount(0).orderCount(0).payCount(0).build();
        HotCategory category = categories.stream().reduce(zeroCategory, HotCategory::add);
        // 写出
        context.write(NullWritable.get(), category);
    }
}