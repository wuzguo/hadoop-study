### 四、Spark核心编程

#### 4.9 累加器

##### 实现原理

累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。

##### 基础编程

1. 系统累加器

```scala
val rdd = sc.makeRDD(List(1, 2, 3, 4))

// 获取系统累加器
val sumAcc = sc.longAccumulator("sum")

//sc.doubleAccumulator
//sc.collectionAccumulator
rdd.foreach(
    num => {
        // 使用累加器
        sumAcc.add(num)
    }
)

// 获取累加器的值
println(sumAcc.value)
```

2. 自定义累加器

```scala
/**
 * 自定义累加器
 */
class CustomAccumulatorV2 extends AccumulatorV2[String, mutable.Map[String, Long]] {
    // 计数
    private val wcMap = mutable.Map[String, Long]()

    // 累加器是否为初始状态
    override def isZero: Boolean = wcMap.isEmpty

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new CustomAccumulatorV2

    // 重置累加器
    override def reset(): Unit = wcMap.clear()

    // 向累加器增加数据
    override def add(v: String): Unit = {
        val value = wcMap.getOrElse(v, 0L) + 1
        wcMap.update(v, value)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
        val values = other.value

        values.foreach {
            case (key, value) =>
            val newCount = wcMap.getOrElse(key, 0L) + value
            wcMap.update(key, newCount)
            case _ =>
        }
    }

    // 返回累加器结果
    override def value: mutable.Map[String, Long] = wcMap
}
```



#### 4.10 广播变量

##### 实现原理

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

##### 基础编程

```scala
val rdd1 = sc.makeRDD(List(
    ("a", 1), ("b", 2), ("c", 3)
))
val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

// 封装广播变量
val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

// 方法广播变量
rdd1.map {
    case (w, c) =>
    val l: Int = bc.value.getOrElse(w, 0)
    (w, (c, l))
}.collect().foreach(println)

// (a,(1,4))
// (b,(2,5))
// (c,(3,6))
```

