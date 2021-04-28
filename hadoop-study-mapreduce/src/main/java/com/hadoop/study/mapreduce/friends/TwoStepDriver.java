package com.hadoop.study.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/28 15:21
 */

public class TwoStepDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 配置文件
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置目录
        job.setJarByClass(TwoStepDriver.class);

        // 2. 设置Map Class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 3. 设置Reduce class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置Mapper Class
        job.setMapperClass(TwoStepMapper.class);
        job.setReducerClass(TwoStepReducer.class);

        // 5 指定job的输入原始文件所在目录   // input
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
