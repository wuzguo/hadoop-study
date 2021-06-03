### 五、Spark核心编程

#### 4.2 累加器

##### 实现原理

累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。

##### 基础编程

1. 系统累加器

```scala
val rdd = sc.makeRDD(List(1,2,3,4,5))
// 声明累加器
var sum = sc.longAccumulator("sum");
rdd.foreach(
num => {
// 使用累 加器
sum.add(num)
// 获取累加器的值
println("sum = " + sum.value)
```

