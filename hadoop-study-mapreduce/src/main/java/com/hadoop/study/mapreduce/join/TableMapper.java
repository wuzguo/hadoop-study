package com.hadoop.study.mapreduce.join;

import com.hadoop.study.mapreduce.domain.TableBean;
import com.hadoop.study.mapreduce.enums.TypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 14:48
 */

@Slf4j
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    // 文件名称
    private String name;

    // 对象
    private final TableBean bean = new TableBean();

    // Value
    private final Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1 获取输入文件切片
        FileSplit split = (FileSplit) context.getInputSplit();
        // 2 获取输入文件名称
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.info("{}, {}", key, value);
        // 1 获取输入文件切片
        FileSplit split = (FileSplit) context.getInputSplit();
        log.info("split name: {}", split.getPath().getName());

        String[] fields = value.toString().split("\t");
        if (name.startsWith(TypeEnum.PRODUCT.getValue())) {
            bean.setFlag(TypeEnum.PRODUCT.getCode());
            bean.setPId(fields[0]);
            bean.setPName(fields[1]);
            bean.setOrderId("");
            bean.setCount(0);
        } else {
            bean.setFlag(TypeEnum.ORDER.getCode());
            bean.setOrderId(fields[0]);
            bean.setCount(Integer.valueOf(fields[2]));
            bean.setPId("");
            bean.setPName("");
        }
        context.write(value, bean);
    }
}
