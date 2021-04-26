package com.hadoop.study.mapreduce.grouping;

import com.hadoop.study.mapreduce.domain.OrderBean;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, Text, FloatWritable> {

    private static final Text text = new Text();

    private static final FloatWritable value = new FloatWritable(0);

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        text.set(String.valueOf(key.getOrderId()));
        value.set(key.getAmount().floatValue());
        context.write(text, value);
    }
}
