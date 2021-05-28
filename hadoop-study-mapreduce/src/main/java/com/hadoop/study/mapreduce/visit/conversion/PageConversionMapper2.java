package com.hadoop.study.mapreduce.visit.conversion;

import java.io.IOException;
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

public class PageConversionMapper2 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {


    private final LongWritable page = new LongWritable();

    private final LongWritable sum = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        String[] values = lines.split("_");

        page.set(Long.parseLong(values[3].trim()));
        context.write(page, sum);
    }
}
