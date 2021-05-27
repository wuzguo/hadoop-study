package com.hadoop.study.mapreduce.visit.session;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 18:39
 */

@Slf4j
public class SessionCategoryDriver {

    public static void main(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 1 获取配置信息，或者job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(SessionCategoryDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(SessionCategoryMapper.class);
        job.setReducerClass(SessionCategoryReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 设置缓存文件
        job.setCacheFiles(new URI[]{new URI("./hadoop-study-datas/spark/output2/part-r-00000")});

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
