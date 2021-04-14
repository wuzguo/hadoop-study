### Hadoop 运行环境搭建

#### 1. 下载Hadoop安装包

hadoop下载地址：[https://downloads.apache.org/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz](https://downloads.apache.org/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz)

#### 2. 上传安装包到服务器

使用Xftp、SecureCRT等工具将hadoop安装包上传到服务器。

![](./images/202104_01/1.png)

#### 3. 解压安装包

```shell
[zak@hadoop001 software]$ ll
total 497624
-rw-rw-r-- 1 zak  zak  366447449 Apr 14 10:49 hadoop-2.9.2.tar.gz
-rw-r--r-- 1 root root 143111803 Apr 10 21:43 jdk-8u261-linux-x64.tar.gz
[zak@hadoop002 software]$ tar -zxvf hadoop-2.9.2.tar.gz -C /opt/module/
```

#### 4. 查看是否解压成功

```shell
[zak@hadoop001 software]$ ll /opt/module/
total 8
drwxr-xr-x 9 zak   zak   4096 Nov 13  2018 hadoop-2.9.2
drwxr-xr-x 8 10143 10143 4096 Jun 18  2020 jdk1.8.0_261
```

#### 5. 将Hadoop添加到环境变量

- 获取Hadoop安装路径

```shell
[zak@hadoop001 hadoop-2.9.2]$ pwd
/opt/module/hadoop-2.9.2
```

- 修改环境变量

```
[zak@hadoop001 hadoop-2.9.2]$ sudo vi /etc/profile
```

- 在profile文件末尾添加Hadoop安装路径

```
##HADOOP_HOME
export HADOOP_HOME=/opt/module/hadoop-2.9.2
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
```

- 保存后退出
- 生效的环境变量

```sh
[zak@hadoop001 hadoop-2.9.2]$ source /etc/profile
```

- 测试功能

```shell
[zak@hadoop001 hadoop-2.9.2]$ hadoop version
Hadoop 2.9.2
......
```

#### 6. Hadoop目录结构

```sh
[zak@hadoop001 hadoop-2.9.2]$ ll
total 164
drwxr-xr-x 2 zak zak   4096 Nov 13  2018 bin
drwxrwxr-x 3 zak zak   4096 Apr 13 21:20 data
drwxr-xr-x 3 zak zak   4096 Nov 13  2018 etc
drwxr-xr-x 2 zak zak   4096 Nov 13  2018 include
drwxrwxr-x 2 zak zak   4096 Apr 13 21:38 input
drwxr-xr-x 3 zak zak   4096 Nov 13  2018 lib
drwxr-xr-x 2 zak zak   4096 Nov 13  2018 libexec
-rw-r--r-- 1 zak zak 106210 Nov 13  2018 LICENSE.txt
drwxrwxr-x 3 zak zak   4096 Apr 13 22:17 logs
-rw-r--r-- 1 zak zak  15917 Nov 13  2018 NOTICE.txt
-rw-r--r-- 1 zak zak   1366 Nov 13  2018 README.txt
drwxr-xr-x 3 zak zak   4096 Nov 13  2018 sbin
drwxr-xr-x 4 zak zak   4096 Nov 13  2018 share
```

#### 7. Hadoop重要目录说明

- bin：存放对Hadoop相关服务（HDFS,YARN）进行操作的脚本

- etc：Hadoop的配置文件目录，存放Hadoop的配置文件

- lib：存放Hadoop的本地库（对数据进行压缩解压缩功能）

- sbin：存放启动或停止Hadoop相关服务的脚本

- share：存放Hadoop的依赖jar包、文档、和官方案例