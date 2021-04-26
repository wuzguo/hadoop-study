package com.hadoop.study.mapreduce.sequence;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2020/7/3 16:59
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration config;

    private FileSplit split;

    private boolean isProgress = true;

    private final BytesWritable value = new BytesWritable();

    private final Text text = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.split = (FileSplit) split;
        config = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 如果不是进行中
        if (!isProgress) {
            return false;
        }

        // 1 定义缓存区
        byte[] contents = new byte[(int) split.getLength()];

        Path path = split.getPath();

        try (FileSystem file = path.getFileSystem(config); FSDataInputStream dataInput = file.open(path)) {
            // 4 读取文件内容
            IOUtils.readFully(dataInput, contents, 0, contents.length);
            // 5 输出文件内容
            value.set(contents, 0, contents.length);
            // 6 获取文件路径及名称
            String name = split.getPath().toString();
            // 7 设置输出的key值
            text.set(name);
        }

        isProgress = false;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
