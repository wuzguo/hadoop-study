package com.hadoop.study.mapreduce.grouping;

import com.hadoop.study.mapreduce.domain.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private static final OrderBean order = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 截取
        String[] fields = line.split("\t");
        // 3 封装对象
        order.setOrderId(Integer.valueOf(fields[0]));
        order.setAmount(Double.parseDouble(fields[2]));
        // 4 写出
        context.write(order, NullWritable.get());
    }
}