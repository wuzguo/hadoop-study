### Hive数据倾斜解决方案

#### 一、问题剖析

数据倾斜是分布式系统不可避免的问题，任何分布式系统都有几率发生数据倾斜，但有些小伙伴在平时工作中感知不是很明显。

#### 二、原因分类

1. 任务需要处理大量相同的key的数据

    在map和reduce两个阶段中，最容易出现数据倾斜的就是reduce阶段，因为map到reduce会经过shuffle阶段，在shuffle中默认会按照key进行hash，如果相同的key过多，那么hash的结果就是大量相同的key进入到同一个reduce中，导致数据倾斜。

2. 任务读取不可分割的大文件

    一个任务中，数据文件在进入map阶段之前会进行切分，默认是128M一个数据块，但是如果当对文件使用GZIP压缩等不支持文件分割操作的压缩方式时，MR任务读取压缩后的文件时，是对它切分不了的，该压缩文件只会被一个任务所读取，如果有一个超大的不可切分的压缩文件被一个map读取时，就会发生map阶段的数据倾斜。

#### 三、案例分析

MapReduce和Spark中的数据倾斜解决方案原理都是类似的，以下讨论Hive使用MapReduce引擎引发的数据倾斜，Spark数据倾斜也可以此为参照。

##### 1. 空值引发的数据倾斜

可以直接不让null值参与join操作，即不让null值有shuffle阶段。

```sql
SELECT *
FROM log a
 JOIN users b
 ON a.user_id IS NOT NULL
  AND a.user_id = b.user_id
UNION ALL
SELECT *
FROM log a
WHERE a.user_id IS NULL;
```

给null值随机赋值，这样它们的hash结果就不一样，就会进到不同的reduce中。

```sql
SELECT *
FROM log a
 LEFT JOIN users b ON CASE 
   WHEN a.user_id IS NULL THEN concat('hive_', rand())
   ELSE a.user_id
  END = b.user_id;
```

##### 2. 不同数据类型引发的数据倾斜

如果key字段既有string类型也有int类型，默认的hash就都会按int类型来分配，那我们直接把int类型都转为string就好了，这样key字段都为string，hash时就按照string类型分配了。

```sql
SELECT *
FROM users a
 LEFT JOIN logs b ON a.usr_id = CAST(b.user_id AS string);
```

##### 3. 不可拆分大文件引发的数据倾斜

这种数据倾斜问题没有什么好的解决方案，只能将使用GZIP压缩等不支持文件分割的文件转为bzip和zip等支持文件分割的压缩方式。

##### 4. 数据膨胀引发的数据倾斜

在多维聚合计算时，如果进行分组聚合的字段过多，如下：

```sql
select a，b，c，count（1）from log group by a，b，c with rollup;
```

注：对于最后的 with rollup 关键字不知道大家用过没，with rollup 是用来在分组统计数据的基础上再进行统计汇总，即用来得到group by的汇总信息。

- 解决方案

可以拆分上面的sql，将with rollup拆分成如下几个sql：

```sql
SELECT a, b, c, COUNT(1)
FROM log
GROUP BY a, b, c;

SELECT a, b, NULL, COUNT(1)
FROM log
GROUP BY a, b;

SELECT a, NULL, NULL, COUNT(1)
FROM log
GROUP BY a;

SELECT NULL, NULL, NULL, COUNT(1)
FROM log;
```

但是，上面这种方式不太好，因为现在是对3个字段进行分组聚合，那如果是5个或者10个字段呢，那么需要拆解的SQL语句会更多。

在Hive中可以通过参数 **hive.new.job.grouping.set.cardinality** 配置的方式自动控制作业的拆解，该参数默认值是30。表示针对 **grouping sets/rollups/cubes** 这类多维聚合的操作，如果最后拆解的键组合大于该值，会启用新的任务去处理大于该值之外的组合。如果在处理数据时，某个分组聚合的列有较大的倾斜，可以适当调小该值。

##### 5. 表连接时引发的数据倾斜

两表进行普通的repartition join时，如果表连接的键存在倾斜，那么在 Shuffle 阶段必然会引起数据倾斜。

- 解决方案

通常做法是将倾斜的数据存到分布式缓存中，分发到各个Map任务所在节点。在Map阶段完成join操作，即MapJoin，这避免了 Shuffle，从而避免了数据倾斜。

MapJoin是Hive的一种优化操作，其适用于小表JOIN大表的场景，由于表的JOIN操作是在Map端且在内存进行的，所以其并不需要启动Reduce任务也就不需要经过shuffle阶段，从而能在一定程度上节省资源提高JOIN效率。

在Hive 0.11版本之前，如果想在Map阶段完成join操作，必须使用MAPJOIN来标记显示地启动该优化操作，由于其需要将小表加载进内存所以要注意小表的大小。

如将a表放到Map端内存中执行，在Hive 0.11版本之前需要这样写：

```sql
select /* +mapjoin(a) */ a.id , a.name, b.age 
from a join b 
on a.id = b.id;
```

如果想将多个表放到Map端内存中，只需在mapjoin()中写多个表名称即可，用逗号分隔，如将a表和c表放到Map端内存中，则 `/*+mapjoin(a,c)*/ `。

在Hive 0.11版本及之后，Hive默认启动该优化，也就是不在需要显示的使用MAPJOIN标记，其会在必要的时候触发该优化操作将普通JOIN转换成MapJoin，可以通过以下两个属性来设置该优化的触发时机：

```xml
# 认值为true，自动开启MAPJOIN优化
hive.auto.convert.join=true

# 默认值为2500000(25M)，通过配置该属性来确定使用该优化的表的大小，如果表的大小小于此值就会被加载进内存中
hive.mapjoin.smalltable.filesize=2500000
```

注意：使用默认启动该优化的方式如果出现莫名其妙的BUG(比如MAPJOIN并不起作用)，就将以下两个属性置为fase手动使用MAPJOIN标记来启动该优化:

```xml
# 关闭自动MAPJOIN转换操作
hive.auto.convert.join=false

# 不忽略MAPJOIN标记
hive.ignore.mapjoin.hint=false
```

再提一句：将表放到Map端内存时，如果节点的内存很大，但还是出现内存溢出的情况，我们可以通过这个参数 **mapreduce.map.memory.mb** 调节Map端内存的大小。

##### 6. 确实无法减少数据量引发的数据倾斜

在一些操作中，我们没有办法减少数据量，如在使用 collect_list 函数时：

```sql
select s_age, collect_list(s_score) list_score
from student
group by s_age
```

collect_list：将分组中的某列转为一个数组返回。

在上述sql中，s_age 如果存在数据倾斜，当数据量大到一定的数量，会导致处理倾斜的reduce任务产生内存溢出的异常。

注：collect_list 输出一个数组，中间结果会放到内存中，所以如果collect_list聚合太多数据，会导致内存溢出。

有小伙伴说这是 group by 分组引起的数据倾斜，可以开启 **hive.groupby.skewindata** 参数来优化。我们接下来分析下：

开启该配置会将作业拆解成两个作业，第一个作业会尽可能将Map的数据平均分配到Reduce阶段，并在这个阶段实现数据的预聚合，以减少第二个作业处理的数据量；第二个作业在第一个作业处理的数据基础上进行结果的聚合。

**hive.groupby.skewindata** 的核心作用在于生成的第一个作业能够有效减少数量。但是对于 collect_list 这类要求全量操作所有数据的中间结果的函数来说，明显起不到作用，反而因为引入新的作业增加了磁盘和网络I/O的负担，而导致性能变得更为低下。

- 解决方案

这类问题最直接的方式就是调整reduce所执行的内存大小。

调整reduce的内存大小使用 **mapreduce.reduce.memory.mb** 这个配置。