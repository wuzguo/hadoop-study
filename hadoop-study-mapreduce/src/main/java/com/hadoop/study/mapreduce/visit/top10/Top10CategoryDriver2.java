package com.hadoop.study.mapreduce.visit.top10;

import com.hadoop.study.mapreduce.domain.HotCategory;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/5/27 14:04
 */

public class Top10CategoryDriver2 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1 获取配置信息，或者job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(Top10CategoryDriver2.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(Top10CategoryMapper2.class);
        job.setReducerClass(Top10CategoryReducer2.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(HotCategory.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 5 指定job的输入原始文件所在目录
        // 输入参数依赖 Top10CategoryDriver 的输出文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}