package com.hadoop.study.mapreduce.etl;

import com.hadoop.study.mapreduce.domain.LogBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 16:33
 */

@Slf4j
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取1行
        String line = value.toString();

        // 2 解析日志是否合法
        LogBean bean = parseLog(line);

        if (bean.getValid()) {
            // 3 输出
            text.set(bean.toString());
            context.write(text, NullWritable.get());
        }
    }

    // 解析日志
    private LogBean parseLog(String line) {
        // 1 截取
        String[] fields = line.split(" ");
        // 如果小于 11 就是不合格的数据
        if (fields.length < 12) {
            return LogBean.builder().valid(false).build();
        }

        // 构造返回结果
        return LogBean.builder().remoteAddr(fields[0])
                .remoteUser(fields[1])
                .timeLocal(fields[3].substring(1))
                .request(fields[6])
                .status(fields[8])
                .bodyBytesSent(fields[9])
                .httpReferer(fields[10])
                .httpUserAgent(fields.length > 12 ? fields[11] + " " + fields[12] : fields[11])
                // 大于400，HTTP错误
                .valid(Integer.parseInt(fields[8]) < 400)
                .build();
    }
}
