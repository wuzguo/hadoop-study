package com.hadoop.study.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.util.Optional;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/4/25 9:47
 */

@Slf4j
@FixMethodOrder
public class TestZookeeper {

    private static final String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";

    private static final int sessionTimeout = 2000;

    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, event -> {
            // 收到事件通知后的回调函数（用户的业务逻辑）
            log.info("{} -- {} ", event.getType(), event.getPath());
            // 再次启动监听
            try {
                zkClient.getChildren("/", true);
            } catch (Exception e) {
                log.error("异常信息：{}", e.getMessage());
            }
        });
    }

    // 创建子节点
    @Test
    public void create() throws Exception {
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/zak", "hello, Zookeeper".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("节点信息： {}", nodeCreated);
    }

    // 获取子节点
    @Test
    public void getChildren() throws Exception {
        Optional.ofNullable(zkClient.getChildren("/", true))
                .ifPresent(children -> children.forEach(child -> log.info("子节点： {}", child)));
    }

    // 判断znode是否存在
    @Test
    public void exist() throws Exception {
        Stat stat = zkClient.exists("/zookeeper", false);
        log.info("节点Zookeeper是否存在： {}", stat != null);
    }
}