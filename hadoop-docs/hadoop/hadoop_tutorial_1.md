### Hadoop 运行模式

Hadoop运行模式包括：本地模式、伪分布式模式以及完全分布式模式。

#### 一. 本地模式

官方案例WordCount演示

- 创建在hadoop-2.7.2文件下面创建一个wcinput文件夹

```shell
[zak@hadoop001 hadoop-2.9.2]$ mkdir input
```

- 在input文件下创建一个1.txt文件

```shell
[zak@hadoop001 hadoop-2.7.2]$ vi input/1.txt
```

- 在文件中输入如下内容

```shell
hadoop yarn
hadoop mapreduce
iot
aiot
```

- 执行程序

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount input output
```

- 查看结果

```shell
[zak@hadoop001 hadoop-2.9.2]$ cat output/part-r-00000
aiot 1
hadoop 2
iot 1
mapreduce    1
yarn  1
```

#### 二. 伪分布式模式

##### 1. 配置集群

- 配置 hadoop-env.sh

```shell
export JAVA_HOME=/opt/module/jdk1.8.0_261
```

- 修改配置文件 core-site.xml

```XML
<!-- 指定HDFS中NameNode的地址 -->
<property>
<name>fs.defaultFS</name>
    <value>hdfs://hadoop001:9000</value>
</property>

<!-- 指定Hadoop运行时产生文件的存储目录 -->
<property>
	<name>hadoop.tmp.dir</name>
	<value>/opt/module/hadoop-2.9.2/data/tmp</value>
</property>

```

- 修改配置文件 hdfs-site.xml

```xml
<!-- 指定HDFS副本的数量 -->
<property>
	<name>dfs.replication</name>
	<value>1</value>
</property>
```

##### 2. 启动集群

- 格式化NameNode

```
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs namenode -format
```

格式化NameNode，会产生新的集群id，导致NameNode和DataNode的集群id不一致，集群找不到已往数据。所以，格式NameNode时，一定要先删除data数据和log日志，然后再格式化NameNode。

- 启动NameNode

```
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh start namenode
```

- 启动DataNode

```
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh start datanode
```

##### 3. 查看集群

- 查看是否启动成功

```
[zak@hadoop001 hadoop-2.9.2]$ jps
30227 NameNode
19268 Jps
30341 DataNode
```

- 浏览器查看HDFS文件系统

http://hadoop001:50070/dfshealth.html#tab-overview

![](./images/202104_01/3.png)

- 查看产生的Log日志

```shell
[zak@hadoop001 hadoop-2.9.2]$ ll logs/
total 9828
-rw-rw-r--   1 zak zak  112437 Apr 14 11:19 hadoop-zak-datanode-hadoop001.log
-rw-rw-r--   1 zak zak     715 Apr 13 22:16 hadoop-zak-datanode-hadoop001.out
-rw-rw-r--   1 zak zak     715 Apr 13 21:21 hadoop-zak-datanode-hadoop001.out.1
-rw-rw-r--   1 zak zak  158093 Apr 14 10:04 hadoop-zak-namenode-hadoop001.log
-rw-rw-r--   1 zak zak    6001 Apr 13 22:17 hadoop-zak-namenode-hadoop001.out
-rw-rw-r--   1 zak zak    5996 Apr 13 21:35 hadoop-zak-namenode-hadoop001.out.1
-rw-rw-r--   1 zak zak  101005 Apr 14 11:23 mapred-zak-historyserver-hadoop001.log
-rw-rw-r--   1 zak zak    1484 Apr 13 22:17 mapred-zak-historyserver-hadoop001.out
-rw-rw-r--   1 zak zak    1484 Apr 13 22:10 mapred-zak-historyserver-hadoop001.out.1
-rw-rw-r--   1 zak zak       0 Apr 13 21:20 SecurityAuth-zak.audit
drwxr-xr-x 182 zak zak   16384 Apr 14 10:00 userlogs
-rw-rw-r--   1 zak zak 5451831 Apr 14 10:01 yarn-zak-nodemanager-hadoop001.log
-rw-rw-r--   1 zak zak    1553 Apr 14 10:01 yarn-zak-nodemanager-hadoop001.out
-rw-rw-r--   1 zak zak    1508 Apr 13 21:59 yarn-zak-nodemanager-hadoop001.out.1
-rw-rw-r--   1 zak zak 4144059 Apr 14 11:01 yarn-zak-resourcemanager-hadoop001.log
-rw-rw-r--   1 zak zak    1531 Apr 13 22:16 yarn-zak-resourcemanager-hadoop001.out
-rw-rw-r--   1 zak zak    1524 Apr 13 21:58 yarn-zak-resourcemanager-hadoop001.out.1
-rw-rw-r--   1 zak zak     760 Apr 13 21:58 yarn-zak-resourcemanagerw-hadoop001.out
```

##### 5. 操作集群

- 在HDFS文件系统上创建一个input文件夹

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs dfs -mkdir -p /user/zak/input
```

- 将测试文件内容上传到文件系统上

```shell
[zak@hadoop001 hadoop-2.9.2]$bin/hdfs dfs -put input/
/user/zak/input/
```

- 查看上传的文件是否正确

```shell
[zak@hadoop001 hadoop-2.9.2]$ ll input/
total 8
-rw-rw-r-- 1 zak zak 44 Apr 13 21:37 1.txt
-rw-rw-r-- 1 zak zak 27 Apr 13 21:38 2.txt
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs dfs -ls  /user/zak/input/
Found 2 items
-rw-r--r--   1 zak supergroup         44 2021-04-13 21:46 /user/zak/input/1.txt
-rw-r--r--   1 zak supergroup         27 2021-04-13 21:46 /user/zak/input/2.txt
```

- 运行MapReduce程序

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount /user/zak/input/ /user/zak/output
```

- 查看输出结果

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs dfs -cat /user/zak/output/*
Dage	1
IoT	1
hadoop	2
iot	1
mapreduce	1
xiaodi	1
yarn	1
[zak@hadoop001 hadoop-2.9.2]$ 
```

- 浏览器查看

![](./images/202104_01/4.png)

##### 6. 使用YARN运行MapReduce程序

- 配置 yarn-env.sh

```shell
export JAVA_HOME=/opt/module/jdk1.8.0_261
```

- 修改配置文件 yarn-site.xml

```xml
<!-- Reducer获取数据的方式 -->
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>

<!-- 指定YARN的ResourceManager的地址 -->
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>hadoop001</value>
</property>
```

- 配置 mapred-env.sh

```sh
export JAVA_HOME=/opt/module/jdk1.8.0_261
```

- 修改配置文件 mapred-site.xml

```shell
[zak@hadoop001 hadoop]$ mv mapred-site.xml.template mapred-site.xml
[zak@hadoop001 hadoop]$ vi mapred-site.xml

<configuration>
  <!-- 指定MR运行在YARN上 -->：
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>

```

##### 7. 启动集群

- 启动ResourceManager

```
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh start resourcemanager
```

- 启动NodeManager

```
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh start nodemanager
```

##### 8. 查看集群服务

```shell
[zak@hadoop001 hadoop-2.9.2]$ jps
22306 Jps
30451 ResourceManager
30227 NameNode
30341 DataNode
30909 JobHistoryServer
```

##### 9. 操作集群

- YARN的浏览器页面查看

http://hadoop101:8088/cluster

![](./images/202104_01/5.png)

- 删除文件系统上的output文件

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs dfs -rm -R /user/zak/output
```

- 执行MapReduce程序

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount /user/zak/input  /user/zak/output
```

- 查看运行结果

```shell
[zak@hadoop001 hadoop-2.9.2]$ bin/hdfs dfs -cat /user/zak/output/*
Dage	1
IoT	1
hadoop	2
iot	1
mapreduce	1
xiaodi	1
yarn	1
[zak@hadoop001 hadoop-2.9.2]$ 
```

- 浏览器查看运行结果

![](./images/202104_01/6.png)

##### 10. 配置历史服务器

为了查看程序的历史运行情况，需要配置一下历史服务器。具体配置步骤如下：

- 修改配置文件 mapred-site.xml

在该文件里面增加如下配置。

```xml
<!-- 历史服务器端地址 -->
<property>
  <name>mapreduce.jobhistory.address</name>
  <value>hadoop001:10020</value>
</property>

<!-- 历史服务器web端地址 -->
<property>
  <name>mapreduce.jobhistory.webapp.address</name>
  <value>hadoop001:19888</value>
</property>
```

- 启动历史服务器

```
[zak@hadoop001 hadoop-2.9.2]$ sbin/mr-jobhistory-daemon.sh start historyserver
```

- 查看服务是否启动

```
[zak@hadoop001 hadoop-2.9.2]$ jps
22306 Jps
30451 ResourceManager
30227 NameNode
30341 DataNode
30909 JobHistoryServer
```

- 查看JobHistory

http://hadoop101:19888/jobhistory

![](./images/202104_01/7.png)

##### 11. 配置日志的聚集

日志聚集概念：应用运行完成以后，将程序运行日志信息上传到HDFS系统上。
日志聚集功能好处：可以方便的查看到程序运行详情，方便开发调试。
注意：开启日志聚集功能，需要重新启动NodeManager 、ResourceManager和HistoryManager。

- 修改配置文件 yarn-site.xml

```xml
<!-- 日志聚集功能使能 -->
<property>
  <name>yarn.log-aggregation-enable</name>
  <value>true</value>
</property>

<!-- 日志保留时间设置7天 -->
<property>
  <name>yarn.log-aggregation.retain-seconds</name>
  <value>604800</value>
</property>
```

- 重启各个服务

```shell
[zak@hadoop001 hadoop-2.9.2]$ sbin/mr-jobhistory-daemon.sh stop historyserver
stopping historyserver
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh stop resourcemanager
stopping resourcemanager
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh stop nodemanager
stopping nodemanager
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh stop namenode
stopping namenode
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh stop datanode
stopping datanode
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh start datanode
starting datanode, logging to /opt/module/hadoop-2.9.2/logs/hadoop-zak-datanode-hadoop001.out
[zak@hadoop001 hadoop-2.9.2]$ sbin/hadoop-daemon.sh start namenode
starting namenode, logging to /opt/module/hadoop-2.9.2/logs/hadoop-zak-namenode-hadoop001.out
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh start resourcemanager
starting resourcemanager, logging to /opt/module/hadoop-2.9.2/logs/yarn-zak-resourcemanager-hadoop001.out
[zak@hadoop001 hadoop-2.9.2]$ sbin/yarn-daemon.sh start nodemanager
starting nodemanager, logging to /opt/module/hadoop-2.9.2/logs/yarn-zak-nodemanager-hadoop001.out
[zak@hadoop001 hadoop-2.9.2]$ sbin/mr-jobhistory-daemon.sh start historyserver
starting historyserver, logging to /opt/module/hadoop-2.9.2/logs/mapred-zak-historyserver-hadoop001.out
[zak@hadoop001 hadoop-2.9.2]$ 
```

- 查看服务状态

```shell
[zak@hadoop001 hadoop-2.9.2]$ jps
8480 JobHistoryServer
6228 NameNode
6134 DataNode
8567 Jps
6362 ResourceManager
6651 NodeManager
[zak@hadoop001 hadoop-2.9.2]$ 
```

- 执行WordCount程序

```
[zak@hadoop001 hadoop-2.9.2]$ hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount /user/zak/input /user/zak/output
```

- 查看执行日志

    http://hadoop001:19888/jobhistory

![](./images/202104_01/8.png)

![](./images/202104_01/9.png)

##### 12. 配置文件说明

Hadoop配置文件分两类：默认配置文件和自定义配置文件，只有用户想修改某一默认配置值时，才需要修改自定义配置文件，更改相应属性值。

- 默认配置文件

| 文件名称           | 文件存放位置                                                 |
| ------------------ | ------------------------------------------------------------ |
| core-default.xml   | share/hadoop/common/hadoop-common-2.9.2.jar/core-default.xml |
| hdfs-default.xml   | share/hadoop/hdfs/hadoop-hdfs-2.9.2.jar/hdfs-default.xml     |
| yarn-default.xml   | share/hadoop/yarn/hadoop-yarn-common-2.9.2.jar/yarn-default.xml |
| mapred-default.xml | share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar/mapred-default.xml |

- 自定义配置文件

| 文件名称        | 文件存放位置 | 说明            |
| --------------- | ------------ | --------------- |
| core-site.xml   | /etc/hadoop  | 基础配置        |
| hdfs-site.xml   | /etc/hadoop  | HDFS的配置      |
| yarn-site.xml   | /etc/hadoop  | YARN的配置      |
| mapred-site.xml | /etc/hadoop  | MapReduce的配置 |

