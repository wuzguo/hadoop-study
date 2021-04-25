package com.hadoop.study.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.junit.FixMethodOrder;
import org.junit.Test;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/25 10:04
 */

@Slf4j
@FixMethodOrder
public class TestDistribute {

    @Test
    public void testServer() throws Exception {
        // 1获取zk连接
        DistributeServer server = new DistributeServer();
        server.getConnect();
        // 2 利用zk连接注册服务器信息
        server.registServer("hadoop001");
        // 3 启动业务功能
        server.business("hadoop001");
    }

    @Test
    public void testClient() throws Exception {
        // 1获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        // 2获取servers的子节点信息，从中获取服务器信息列表
        client.getServerList();
        // 3业务进程启动
        client.business();
    }
}
