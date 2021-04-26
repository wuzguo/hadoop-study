package com.hadoop.study.mapreduce.serialization;

import com.hadoop.study.mapreduce.domain.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/26 17:01
 */

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

   private FlowBean flowBean = new FlowBean();

   private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        text.set(phoneNum);
        flowBean.set(downFlow, upFlow);
        // 4 写出
        context.write(text, flowBean);

    }
}
