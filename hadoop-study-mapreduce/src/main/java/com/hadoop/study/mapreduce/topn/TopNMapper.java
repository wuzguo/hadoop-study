package com.hadoop.study.mapreduce.topn;

import com.hadoop.study.mapreduce.domain.FlowBean2;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 14:28
 */

@Slf4j
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean2, Text> {

    private final FlowBean2 flowBean = new FlowBean2();

    private final Text text = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.info("{}, {}", key, value);
        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[0];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        text.set(phoneNum);
        flowBean.set(downFlow, upFlow);
        // 4 写出
        context.write(flowBean, text);
    }
}
