package com.hadoop.study.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

/**
 * <B>说明：</B><BR>
 *
 * @author zak.wu
 * @version 1.0.0
 * @date 2020/8/21 15:28
 */

@FixMethodOrder
public class TestHbase {

    private Configuration conf = null;
    private Connection conn = null;

    @Before
    public void config() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop001:2181,hadoop002:2181,hadoop003:2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void closeConn() throws IOException {
        conn.close();
    }

    @Test
    public void testTableExist() throws IOException {
        Table table = conn.getTable(TableName.valueOf("student"));
        Assert.assertNotNull(table);
    }

    @Test
    public void testCreateTable() throws IOException {
        Admin admin = conn.getAdmin();
        if (!admin.isTableAvailable(TableName.valueOf("user"))) {
            TableName tableName = TableName.valueOf("user");
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("addr"));
            //获得列描述起
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }
        //关闭链接
    }

    @Test
    public void testInsertOne() throws IOException {
        //new 一个列  ，hgs_000为row key
        Put put = new Put(Bytes.toBytes("hgs_000"));
        //下面三个分别为，列族，列名，列值
        put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("name"), Bytes.toBytes("zak"));
        TableName tableName = TableName.valueOf("user");
        //得到 table
        Table table = conn.getTable(tableName);
        //执行插入
        table.put(put);
        table.close();
    }

    //插入多个列
    @Test
    public void  testInsertMany() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("hgs_001"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("name"), Bytes.toBytes("zak"));

        Put put2 = new Put(Bytes.toBytes("hgs_001"));
        put2.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("age"), Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("hgs_001"));
        put3.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("hgs_001"));
        put4.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    //同一条数据的插入
    @Test
    public void singleRowInsert() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Put put1 = new Put(Bytes.toBytes("hgs_005"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("name"), Bytes.toBytes("cm"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("weight"), Bytes.toBytes("88kg"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        table.put(put1);
        table.close();
    }

    //数据的更新,hbase对数据只有追加，没有更新，但是查询的时候会把最新的数据返回给哦我们
    @Test
    public void updateData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Put put1 = new Put(Bytes.toBytes("hgs_002"));
        put1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("weight"), Bytes.toBytes("63kg"));
        table.put(put1);
        table.close();
    }

    //删除数据
    @Test
    public void deleteData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        //参数为 row key
        //删除一列
        Delete delete1 = new Delete(Bytes.toBytes("hgs_000"));
        delete1.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("weight"));
        //删除多列
        Delete delete2 = new Delete(Bytes.toBytes("hgs_001"));
        delete2.addColumns(Bytes.toBytes("addr"), Bytes.toBytes("age"));
        delete2.addColumns(Bytes.toBytes("addr"), Bytes.toBytes("sex"));
        //删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("hgs_002"));
        delete3.addFamily(Bytes.toBytes("addr"));

        //删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("hgs_003"));
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);
        table.close();
    }

    //查询
    //
    @Test
    public void querySingleRow() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        //获得一行
        Get get = new Get(Bytes.toBytes("hgs_000"));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell : cells) {
            System.out.println(
                Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
    }

    //全表扫描
    @Test
    public void scanTable() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
        //scan.setStartRow(Bytes.toBytes("wangsf_0"));
        //scan.setStopRow(Bytes.toBytes("wangwu"));
        ResultScanner rsacn = table.getScanner(scan);
        for (Result rs : rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }
    //过滤器

    @Test
    //列值过滤器
    public void singColumnFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("addr"), Bytes.toBytes("name"),
            CompareOperator.EQUAL, Bytes.toBytes("wd"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //row key过滤器
    @Test
    public void rowkeyFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^hgs_00*"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //列名前缀过滤器
    @Test
    public void columnPrefixFilter() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //过滤器集合
    @Test
    public void FilterSet() throws IOException {
        Table table = conn.getTable(TableName.valueOf("user"));
        Scan scan = new Scan();
        FilterList list = new FilterList(Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("addr"), Bytes.toBytes("age"),
            CompareOperator.GREATER, Bytes.toBytes("23"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("weig"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }

}
