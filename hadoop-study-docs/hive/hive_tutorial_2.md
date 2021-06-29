### 四、DDL数据定义

#### 4.1 创建数据库

```sql
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];
```

1. 创建一个数据库，数据库在HDFS上的默认存储路径是/user/hive/warehouse/*.db。

```sql
hive> create database db_hive;
```

2. 避免要创建的数据库已经存在错误，增加if not exists判断。

```sql
hive> create database iot;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Database iot already exists
hive> 
```

3. 创建一个数据库，指定数据库在HDFS上存放的位置

```
hive (default)> create database db_hive2 location '/db_hive2.db';
```

#### 4.2 创建表

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 
[(col_name data_type [COMMENT col_comment], ...)] 
[COMMENT table_comment] 
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 
[CLUSTERED BY (col_name, col_name, ...) 
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 
[ROW FORMAT row_format] 
[STORED AS file_format] 
[LOCATION hdfs_path]
[TBLPROPERTIES (property_name=property_value, ...)]
[AS select_statement]
```

##### 字段解释说明

1. CREATE TABLE 创建一个指定名字的表。如果相同名字的表已经存在，则抛出异常；用户可以用 IF NOT EXISTS 选项来忽略这个异常。

2. EXTERNAL关键字可以让用户创建一个外部表，在建表的同时可以指定一个指向实际数据的路径（LOCATION），在删除表的时候，内部表的元数据和数据会被一起删除，而外部表只删除元数据，不删除数据。

3. COMMENT：为表和列添加注释。

4. PARTITIONED BY 创建分区表。

5. CLUSTERED BY 创建分桶表。

6. SORTED BY不常用，对桶中的一个或多个列另外排序。

7. ROW FORMAT DELIMITED 

      [FIELDS TERMINATED BY char] 

      [COLLECTION ITEMS TERMINATED BY char]

      [MAP KEYS TERMINATED BY char]

      [LINES TERMINATED BY char] 

      | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]

    用户在建表的时候可以自定义SerDe或者使用自带的SerDe。

    如果没有指定ROW FORMAT 或者ROW FORMAT DELIMITED，将会使用自带的SerDe。

    在建表的时候，用户还需要为表指定列，用户在指定表的列的同时也会指定自定义的SerDe，Hive通过SerDe确定表的具体的列的数据。

   SerDe是Serialize/Deserilize的简称，hive使用Serde进行行对象的序列与反序列化。

8. STORED AS指定存储文件类型

    常用的存储文件类型：SEQUENCEFILE（二进制序列文件）、TEXTFILE（文本）、RCFILE（列式存储格式文件）

    如果文件数据是纯文本，可以使用STORED AS TEXTFILE。如果数据需要压缩，使用 STORED AS SEQUENCEFILE。

9. LOCATION ：指定表在HDFS上的存储位置。

10. AS：后跟查询语句，根据查询结果创建表。

11. LIKE允许用户复制现有的表结构，但是不复制数据。

##### 管理表

默认创建的表都是所谓的管理表，有时也被称为内部表。因为这种表，Hive会（或多或少地）控制着数据的生命周期。

Hive默认情况下会将这些表的数据存储在由配置项hive.metastore.warehouse.dir(例如 /user/hive/warehouse)所定义的目录的子目录下。 

当我们删除一个管理表时，Hive也会删除这个表中数据。管理表不适合和其他工具共享数据。

1. 创建管理表

```sql
create table if not exists student(
    id int,
    name string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/hive/warehouse/student';
```

2. 查询表的类型

```shell
hive> desc formatted student;
// ......                  	 
Location:           	hdfs://hadoop001:9000/user/hive/warehouse/student	 
Table Type:         	MANAGED_TABLE   
```

##### 外部表

因为表是外部表，所以Hive并非认为其完全拥有这份数据。删除该表并不会删除掉这份数据，不过描述表的元数据信息会被删除掉。

1. 管理表和外部表的使用场景

每天将收集到的网站日志定期流入HDFS文本文件。在外部表（原始日志表）的基础上做大量的统计分析，用到的中间表、结果表使用内部表存储，数据通过SELECT+INSERT进入内部表。

2. 创建外部表

```sql
create external table stu_external(
id int, 
name string) 
row format delimited fields terminated by '\t' 
location '/user/hive/warehouse/stu_external';
```

3. 查看表格式化数据

```shell
hive> desc formatted stu_external;
// ......                  	 
Location:           	hdfs://hadoop001:9000/user/hive/warehouse/stu_external	 
Table Type:         	EXTERNAL_TABLE 
```

##### 管理表与外部表的互相转换

1. 查询表的类型

```shell
hive> desc formatted student;

Table Type:       MANAGED_TABLE
```

2. 修改内部表student2为外部表

```shell
alter table student set tblproperties('EXTERNAL'='TRUE');
```

3. 查询表的类型

```shell
hive> desc formatted student;

Table Type:       EXTERNAL_TABLE
```

4. 修改外部表student2为内部表

```shell
alter table student2 set tblproperties('EXTERNAL'='FALSE');
```

5. 查询表的类型

```shell
hive> desc formatted student;

Table Type:       MANAGED_TABLE
```

注意：**('EXTERNAL'='TRUE')** 和 **('EXTERNAL'='FALSE')** 为固定写法，区分大小写！

### 五、DML数据操作

#### 5.1 数据导入

##### 向表中装载数据

```shell
hive> load data [local] inpath '/opt/module/datas/student.txt' [overwrite] into table student [partition (partcol1=val1,…)];
```

1. load data: 表示加载数据。

2. local: 表示从本地加载数据到Hive表，否则从HDFS加载数据到Hive表。

3. inpath: 表示加载数据的路径。

4. overwrite: 表示覆盖表中已有数据，否则表示追加。

5. into table: 表示加载到哪张表。

6. student: 表示具体的表。

7. partition: 表示上传到指定分区。

##### 实操案例

1. 创建一张表

```shell
hive> create table student(id string, name string) row format delimited fields terminated by '\t';
```

2. 加载本地文件到Hive

```shell
hive> load data local inpath '/opt/module/datas/student.txt' into table default.student;
```

3. 上传文件到HDFS

```shell
hive> dfs -put /opt/module/datas/student.txt /user/hive/warehouse;
```

4. 加载HDFS文件到hive中

```shell
hive> load data inpath '/user/hive/warehouse/student.txt' into table default.student;
```

5. 加载数据覆盖表中已有的数据

```shell
hive> load data inpath '/user/hive/warehouse/student.txt' overwrite into table default.student;
```

#### 5.2 数据导出

##### Insert导出

1. 将查询的结果导出到本地

```shell
hive> insert overwrite local directory '/opt/module/datas/export/student' select * from student;
```

2. 将查询的结果格式化导出到本地

```shell
hive>insert overwrite local directory '/opt/module/datas/export/student' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  select * from student;
```

3. 将查询的结果导出到HDFS上(没有local)

```shell
hive> insert overwrite directory '/user/student2' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  select * from student;
```

##### Hadoop命令导出到本地

```shell
hive> dfs -get /user/hive/warehouse/student/month=201709/000000_0  /opt/module/datas/export/student.txt;
```

##### Hive Shell命令导出

基本语法：（hive -f/-e 执行语句或者脚本 > file）

```shell
[zak@hadoop002 apache-hive-2.3.7]$ bin/hive -e 'select * from iot.users;' > /opt/module/apache-hive-2.3.7/datas/users.txt;
```

##### Export导出到HDFS上

```shell
hive> export table default.student to '/user/hive/warehouse/export/student';
```

