package com.hadoop.study.mapreduce.outputformat;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream blackAddr = null;

    private FSDataOutputStream whiteAddr = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        // 1 获取文件系统
        try (FileSystem fs = FileSystem.get(job.getConfiguration())) {
            String outPath = job.getConfiguration().get(FileOutputFormat.OUTDIR);
            // 3 创建输出流
            blackAddr = fs.create(new Path(String.format("%s/black.log", outPath)));
            whiteAddr = fs.create(new Path(String.format("%s/white.log", outPath)));
        } catch (IOException e) {
            log.error("异常信息： {}", e.getMessage());
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断是否包含"google" 输出到不同文件
        if (key.toString().contains("google")) {
            blackAddr.write(key.toString().getBytes());
            whiteAddr.write("\n".getBytes(StandardCharsets.UTF_8));
        } else {
            whiteAddr.write(key.toString().getBytes());
            whiteAddr.write("\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭资源
        IOUtils.closeStream(blackAddr);
        IOUtils.closeStream(whiteAddr);
    }
}
