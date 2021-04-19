package com.hadoop.study.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
@FixMethodOrder
public class HdfsFileTest {

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("d:/chrome/apache-maven-3.8.1-bin.tar.gz"));
        // 3 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/user/apache-maven-3.8.1-bin.tar.gz"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");

        // 2 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/apache-maven-3.8.1-bin.tar.gz"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/apache-maven-3.8.1-bin.tar.gz"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");

        // 2 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/apache-maven-3.8.1-bin.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/apache-maven-3.8.1-bin.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for (int i = 0; i < 1024 * 4; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fileSystem.close();
    }

    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");

        // 2 打开输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/apache-maven-3.8.1-bin.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024 * 1024 * 4);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/apache-maven-3.8.1-bin.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fileSystem.close();
    }

}
