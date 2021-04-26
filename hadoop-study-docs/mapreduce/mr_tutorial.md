### 一、MapReduce概述

#### 定义

MapReduce是一个分布式运算程序的编程框架，是用户开发“基于Hadoop的数据分析应用”的核心框架。MapReduce核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发运行在一个Hadoop集群上。

#### 优缺点

##### 优点

- MapReduce易于编程

它简单的实现一些接口，就可以完成一个分布式程序，这个分布式程序可以分布到大量廉价的PC机器上运行。也就是说你写一个分布式程序，跟写一个简单的串行程序是一模一样的。就是因为这个特点使得MapReduce编程变得非常流行。

- 良好的扩展性

当你的计算资源不能得到满足的时候，你可以通过简单的增加机器来扩展它的计算能力。

- 高容错性

MapReduce设计的初衷就是使程序能够部署在廉价的PC机器上，这就要求它具有很高的容错性。比如其中一台机器挂了，它可以把上面的计算任务转移到另外一个节点上运行，不至于这个任务运行失败，而且这个过程不需要人工参与，而完全是由Hadoop内部完成的。

- 适合PB级以上海量数据的离线处理

可以实现上千台服务器集群并发工作，提供数据处理能力。

##### 缺点

- 不擅长实时计算

MapReduce无法像MySQL一样，在毫秒或者秒级内返回结果。

- 不擅长流式计算

MapReduce无法像MySQL一样，在毫秒或者秒级内返回结果。

- 不擅长DAG（有向图）计算

多个应用程序存在依赖关系，后一个应用程序的输入为前一个的输出。在这种情况下，MapReduce并不是不能做，而是使用后，每个MapReduce作业的输出结果都会写入到磁盘，会造成大量的磁盘IO，导致性能非常的低下。

#### 核心思想

1. 分布式的运算程序往往需要分成至少2个阶段。

2. 第一个阶段的MapTask并发实例，完全并行运行，互不相干。

3. 第二个阶段的ReduceTask并发实例互不相干，但是他们的数据依赖于上一个阶段的所有MapTask并发实例的输出。

4. MapReduce编程模型只能包含一个Map阶段和一个Reduce阶段，如果用户的业务逻辑

#### MapReduce进程

一个完整的MapReduce程序在分布式运行时有三类实例进程：

1. MrAppMaster： 负责整个程序的过程调度及状态协调。
2. MapTask：负责Map阶段的整个数据处理流程。
3. ReduceTask： 负责Reduce阶段的整个数据处理流程。

#### 常用数据序列化类型

| Java类型 | Hadoop Writable类型 |
| -------- | ------------------- |
| Boolean  | BooleanWritable     |
| Byte     | ByteWritable        |
| Int      | IntWritable         |
| Float    | FloatWritable       |
| Long     | LongWritable        |
| Double   | DoubleWritable      |
| String   | Text                |
| Map      | MapWritable         |
| Array    | ArrayWritable       |

### 二、Hadoop序列化

#### 什么是序列化

序列化就是把内存中的对象，转换成字节序列（或其他数据传输协议）以便于存储到磁盘（持久化）和网络传输。 

反序列化就是将收到字节序列（或其他数据传输协议）或者是磁盘的持久化数据，转换成内存中的对象。

#### Hadoop序列化特点

1. 紧凑：高效使用存储空间。
2. 快速：读写数据的额外开销小。
3. 可扩展：随着通信协议的升级而可升级。
4. 互操作：支持多语言的交互。

#### 实现序列化接口（Writable）

```java
@Data
public class FlowBean implements Writable {

    /**
     * 上行流量
     */
    private Long upFlow;

    /**
     * 下行流量
     */
    private Long downFlow;

    /**
     * 总流量
     */
    private Long sumFlow;

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(upFlow);
        output.writeLong(downFlow);
        output.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.upFlow  = input.readLong();
        this.downFlow = input.readLong();
        this.sumFlow = input.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }
}
```

