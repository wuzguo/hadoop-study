package com.hadoop.study.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@FixMethodOrder
public class HdfsTest {

    FileSystem fileSystem = null;

    @Before
    public void crateFileSytem() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");
    }

    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testCreateDir() throws IOException {
        // 2 创建目录
        fileSystem.mkdirs(new Path("/user/client/dd"));
    }

    @Test
    public void testDelete() throws IOException {
        // 2 执行删除
        fileSystem.delete(new Path("/user/client/huang"), true);
    }


    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("D:/docs/market.sql"), new Path("/user/market.sql"));
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");
        // 2 上传文件
        fs.copyToLocalFile(new Path("/user/input/1.txt"), new Path("D:/docs/1.txt"));
        // 3 关闭资源
        fs.close();
    }
}
