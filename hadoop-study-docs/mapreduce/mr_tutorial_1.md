### MapReduce框架原理

#### InputFormat数据输入

![](../images/202104_01/30.png)

##### 切片与MapTask并行度决定机制

1. 数据块：Block是HDFS物理上把数据分成一块一块。

2. 数据切片：数据切片只是在逻辑上对输入进行分片，并不会在磁盘上将其切分成片进行存储。

![](../images/202104_01/31.png)

##### FileInputFormat切片机制

- 简单地按照文件的内容长度进行切片。

- 切片大小，默认等于Block大小

- 切片时不考虑数据集整体，而是逐个针对每一个文件单独切片

1. 输入数据有两个文件：

    file1.txt  320M

    file2.txt  10M

2. 经过FileInputFormat的切片机制运算后，形成的切片信息如下：

    file1.txt.split1-- 0~128

    file1.txt.split2-- 128~256

    file1.txt.split3-- 256~320

    file2.txt.split1-- 0~10M

##### FileInputFormat切片大小的参数配置

1. 源码中计算切片大小的公式

```java
Math.max(minSize, Math.min(maxSize, blockSize)); 
mapreduce.input.fileinputformat.split.minsize=1  // 默认值为1
mapreduce.input.fileinputformat.split.maxsize= Long.MAXValue  // 默认值Long.MAXValue
```

因此，默认情况下，切片大小=blocksize。

2. 切片大小设置
    maxsize（切片最大值）：参数如果调得比blockSize小，则会让切片变小，而且就等于配置的这个参数的值。

    minsize（切片最小值）：参数调的比blockSize大，则可以让切片变得比blockSize还大。

3. 获取切片信息API

```java
// 获取切片的文件名称
String name = inputSplit.getPath().getName();
// 根据文件类型获取切片信息
FileSplit inputSplit = (FileSplit) context.getInputSplit();
```

##### CombineTextInputFormat切片机制

框架默认的TextInputFormat切片机制是对任务按文件规划切片，不管文件多小，都会是一个单独的切片，都会交给一个MapTask，这样如果有大量小文件，就会产生大量的MapTask，处理效率极其低下。

1. 应用场景

    CombineTextInputFormat用于小文件过多的场景，它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个MapTask处理。

2. 虚拟存储切片最大值设置

    CombineTextInputFormat.setMaxInputSplitSize(job, 4194304); // 4m
    注意：虚拟存储切片最大值设置最好根据实际的小文件大小情况来设置具体的值。

3. 切片机制
    生成切片过程包括：虚拟存储过程和切片过程二部分。

![](../images/202104_01/32.png)

- 虚拟存储过程：

    将输入目录下所有文件大小，依次和设置的setMaxInputSplitSize值比较，如果不大于设置的最大值，逻辑上划分一个块。如果输入文件大于设置的最大值且大于两倍，那么以最大值切割一块；当剩余数据大小超过设置的最大值且不大于最大值2倍，此时将文件均分成2个虚拟存储块（防止出现太小切片）。

    例如setMaxInputSplitSize值为4M，输入文件大小为8.02M，则先逻辑上分成一个4M。剩余的大小为4.02M，如果按照4M逻辑划分，就会出现0.02M的小的虚拟存储文件，所以将剩余的4.02M文件切分成（2.01M和2.01M）两个文件。

- 切片过程：

（a）判断虚拟存储的文件大小是否大于setMaxInputSplitSize值，大于等于则单独形成一个切片。

（b）如果不大于则跟下一个虚拟存储文件进行合并，共同形成一个切片。

（c）测试举例：有4个小文件大小分别为1.7M、5.1M、3.4M以及6.8M这四个小文件，则虚拟存储之后形成6个文件块，大小分别为：

   1.7M，（2.55M、2.55M），3.4M，（3.4M、3.4M）

 最终会形成3个切片，大小分别为：

  （1.7+2.55）M，（2.55+3.4）M，（3.4+3.4）M