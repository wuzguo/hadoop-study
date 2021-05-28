package com.hadoop.study.mapreduce.visit.conversion;


import com.hadoop.study.mapreduce.domain.PageAction;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
 * @date 2021/5/28 8:47
 */

public class PageConversionDriver {

    public static void main(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置Class
        job.setJarByClass(PageConversionDriver.class);
        job.setMapperClass(PageConversionMapper.class);
        job.setReducerClass(PageConversionReducer.class);

        // 设置类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageAction.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置名称
        job.setJobName("pageConversionRate");

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 设置缓存文件
        job.setCacheFiles(new URI[]{new URI("./hadoop-study-datas/spark/output5/part-r-00000")});

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
