package com.hadoop.study.recommend.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

@Slf4j
public class HbaseClient {

    private static Admin admin;

    public static Connection connection;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        conf.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        System.out.println(Property.getStrValue("hbase.rootdir"));
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error("error: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param columnFamilies 列族
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            log.info("table exists");
            return;
        }

        log.info("start create table");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
        Arrays.stream(columnFamilies)
            .forEach(columnFamily -> tableDescriptor.addFamily(new HColumnDescriptor(columnFamily)));
        admin.createTable(tableDescriptor);
        log.info("create table success");

    }

    /**
     * 获取一列获取一行数据
     *
     * @param tableName  表名
     * @param rowKey     ROW Key
     * @param famliyName 组名
     * @param column     列
     * @return {@link String}
     * @throws IOException
     */
    public static String getData(String tableName, String rowKey, String famliyName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);

        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(famliyName.getBytes(), column.getBytes());

        return resultValue == null ? null : new String(resultValue);
    }


    /**
     * 获取一行的所有数据并且排序
     *
     * @param tableName 表名
     * @param rowKey    列名
     * @throws IOException
     */
    public static List<Map.Entry> getRow(String tableName, String rowKey) throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\Program\\hadoop");
        Table table = connection.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);

        HashMap<String, Double> maps = Maps.newHashMap();
        if (result.isEmpty()) {
            return null;
        }

        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            maps.put(key, new Double(value));
        }

        List<Map.Entry> ans = Lists.newArrayList();
        ans.addAll(maps.entrySet());
        Collections.sort(ans, (pre, next) -> new Double((Double) pre.getValue() - (Double) next.getValue()).intValue());
        return ans;
    }

    /**
     * 向对应列添加数据
     *
     * @param tableName  表名
     * @param rowKey     行号
     * @param familyName 列族名
     * @param column     列名
     * @param data       数据
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String familyName, String column, String data)
        throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes());
        table.put(put);
    }

    /**
     * 将该单元格加1
     *
     * @param tableName  表名
     * @param rowKey     行号
     * @param familyName 列族名
     * @param column     列名
     * @throws Exception
     */
    public static void incrColumn(String tableName, String rowKey, String familyName, String column) throws Exception {
        String val = getData(tableName, rowKey, familyName, column);
        int res = 1;
        if (val != null) {
            res = Integer.parseInt(val) + 1;
        }
        putData(tableName, rowKey, familyName, column, String.valueOf(res));
    }

    /**
     * 取出表中所有的key
     *
     * @param tableName 表名
     * @return {@link String}
     * @throws Exception
     */
    public static List<String> getAllKey(String tableName) throws IOException {
        List<String> keys = Lists.newArrayList();

        Scan scan = new Scan();
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            keys.add(new String(result.getRow()));
        }
        return keys;
    }
}
