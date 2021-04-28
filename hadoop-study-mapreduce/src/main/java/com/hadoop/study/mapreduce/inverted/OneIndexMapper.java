package com.hadoop.study.mapreduce.inverted;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
 * @date 2021/4/28 11:36
 */

@Slf4j
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text text = new Text();

    private String fileName = "";

    private LongWritable oneValue = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
        log.info("fileName: {}", fileName);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        for (String field : fields) {
            if (!StringUtils.isBlank(field)) {
                text.set(String.format("%s\t%s", field, fileName));
                context.write(text, oneValue);
            }
        }
    }
}
