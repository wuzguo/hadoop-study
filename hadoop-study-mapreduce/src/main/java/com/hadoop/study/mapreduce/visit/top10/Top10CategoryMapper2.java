package com.hadoop.study.mapreduce.visit.top10;

import com.hadoop.study.mapreduce.domain.HotCategory;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class Top10CategoryMapper2 extends Mapper<LongWritable, Text, NullWritable, HotCategory> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        String[] values = lines.split(":");
        // 点击，下单，支付品类
        String[] categories = values[1].split(",");

        HotCategory category = HotCategory.builder().categoryId(Long.valueOf(values[0].trim()))
            .clickCount(Integer.valueOf(categories[0].trim()))
            .orderCount(Integer.valueOf(categories[1].trim()))
            .payCount(Integer.valueOf(categories[2].trim())).build();

        // 写出
        context.write(NullWritable.get(), category);
    }
}