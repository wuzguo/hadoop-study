package com.hadoop.study.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");
        // 2 创建目录
        fs.mkdirs(new Path("/user/client/zhang"));
        // 3 关闭资源
        fs.close();
    }
}
