package com.hadoop.study.hdfs;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
@FixMethodOrder
public class HdfsTest {

    private FileSystem fileSystem = null;

    @Before
    public void crateFileSystem() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "zak");
        log.info("启动成功");
    }

    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
        log.info("完成关闭");
    }

    @Test
    public void testCreateDir() throws IOException {
        // 创建目录
        fileSystem.mkdirs(new Path("/user/client/dir"));
    }

    @Test
    public void testDelete() throws IOException {
        // 执行删除
        fileSystem.delete(new Path("/user/client/dir"), true);
    }


    @Test
    public void testCopyFromLocalFile() throws IOException {
        // 上传文件
        fileSystem.copyFromLocalFile(new Path("D:/docs/market.sql"), new Path("/user/market.sql"));
    }

    @Test
    public void testCopyToLocalFile() throws IOException {
        // 下载文件
        fileSystem.copyToLocalFile(new Path("/user/input/1.txt"), new Path("D:/docs/1.txt"));
    }

    @Test
    public void testRename() throws IOException {
        // 修改文件名称
        fileSystem.rename(new Path("/user/1.txt"), new Path("/user/rename.txt"));
    }

    @Test
    public void testListFiles() throws IOException {
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }

    @Test
    public void testListStatus() throws IOException {
        // 判断是文件还是文件夹
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/user"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }
}
