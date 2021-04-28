package com.hadoop.study.mapreduce.join.map;

import com.hadoop.study.mapreduce.domain.TableBean;
import com.hadoop.study.mapreduce.join.reduce.TableDriver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/27 15:43
 */

@Slf4j
public class TableCacheDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(TableCacheDriver.class);

        // 3 指定本业务job要使用的Mapper/Reducer业务类
        job.setMapperClass(TableCacheMapper.class);

        // 4 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 指定job的输入原始文件所在目录   // input5
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 设置缓存文件
        String filePath = StringUtils.replace("file:///" + args[0] + "/product.txt", "\\", "/");
        job.setCacheFiles(new URI[]{new URI(filePath)});

        // 7 设置输出的ReducerTask文件，这样不需要ReduceTask阶段
        job.setNumReduceTasks(0);

        // 8 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
