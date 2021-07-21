package com.hadoop.study.recommend.config;

import com.hadoop.study.recommend.hbase.HbaseTemplate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <B>说明：描述</B>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2021/7/21 15:34
 */

@Configuration
@ConditionalOnClass(HbaseTemplate.class)
public class HbaseConfig {

    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";

    private static final String HBASE_ROOTDIR = "hbase.rootdir";

    private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";

    private static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";

    private static final String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";

    private static final String HBASE_CLIENT_OPERATION_TIMEOUT = "hbase.client.operation.timeout";

    private static final String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = "hbase.client.scanner.timeout.period";

    private static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    private static final String HBASE_REST_SSL_ENABLED = "hbase.rest.ssl.enabled";

    private static final String HBASE_CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";

    private static final String HBASE_MASTER = "hbase.master";

    @Autowired
    private HbaseProperties hbaseProperties;

    @Bean
    public HbaseTemplate hbaseTemplate() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_MASTER, hbaseProperties.getMaster());
        configuration.set(HBASE_QUORUM, hbaseProperties.getQuorum());
        configuration.set(HBASE_ROOTDIR, hbaseProperties.getRootDir());
        configuration.set(HBASE_ZNODE_PARENT, hbaseProperties.getZnodeParent());
        configuration.set(ZOOKEEPER_SESSION_TIMEOUT, String.valueOf(hbaseProperties.getSessionTimeout()));
        configuration.set(HBASE_RPC_TIMEOUT, String.valueOf(hbaseProperties.getRpcTimeout()));
        configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, String.valueOf(hbaseProperties.getOperationTimeout()));
        configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, String.valueOf(hbaseProperties.getScannerTimeout()));
        configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, String.valueOf(hbaseProperties.getClientPort()));
        configuration.set(HBASE_REST_SSL_ENABLED, hbaseProperties.getSslEnabled().toString());
        configuration.set(HBASE_CLUSTER_DISTRIBUTED, hbaseProperties.getDistributed().toString());
        return new HbaseTemplate(configuration);
    }
}
