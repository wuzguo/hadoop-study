### 三、Hive数据类型

#### 3.1 基本数据类型

| Hive数据类型 | Java数据类型 | 长度                                                 | 例子              |
| ------------ | ------------ | ---------------------------------------------------- | ----------------- |
| TINYINT      | byte         | 1byte有符号整数                                      | 20                |
| SMALINT      | short        | 2byte有符号整数                                      | 20                |
| INT          | int          | 4byte有符号整数                                      | 20                |
| BIGINT       | long         | 8byte有符号整数                                      | 20                |
| BOOLEAN      | boolean      | 布尔类型，true或者false                              | TRUE FALSE        |
| FLOAT        | float        | 单精度浮点数                                         | 3.14159           |
| DOUBLE       | double       | 双精度浮点数                                         | 3.14159           |
| STRING       | string       | 字符系列。可以指定字符集。可以使用单引号或者双引号。 | “now is the time” |
| TIMESTAMP    | -            | 时间类型                                             | -                 |
| BINARY       | -            | 字节数组                                             | -                 |

对于Hive的String类型相当于数据库的varchar类型，该类型是一个可变的字符串，不过它不能声明其中最多能存储多少个字符，理论上它可以存储2GB的字符数。

#### 3.2 集合数据类型

| 数据类型 | 描述                                                         | 语法示例                                         |
| -------- | ------------------------------------------------------------ | ------------------------------------------------ |
| STRUCT   | 和c语言中的struct类似，都可以通过“点”符号访问元素内容。例如，如果某个列的数据类型是STRUCT{first STRING, last STRING},那么第1个元素可以通过字段.first来引用。 | struct() 例如struct<street:string,  city:string> |
| MAP      | MAP是一组键-值对元组集合，使用数组表示法可以访问数据。例如，如果某个列的数据类型是MAP，其中键->值对是’first’->’John’和’last’->’Doe’，那么可以通过字段名[‘last’]获取最后一个元素 | map() 例如 map<string, int>                      |
| ARRAY    | 数组是一组具有相同类型和名称的变量的集合。这些变量称为数组的元素，每个数组元素都有一个编号，编号从零开始。例如，数组值为[‘John’,  ‘Doe’]，那么第2个元素可以通过数组名[1]进行引用。 | Array() 例如 array<string>                       |

Hive有三种复杂数据类型ARRAY、MAP 和 STRUCT。ARRAY和MAP与Java中的Array和Map类似，而STRUCT与C语言中的Struct类似，它封装了一个命名字段集合，复杂数据类型允许任意层次的嵌套。

##### 案例实操

1. 假设某表有如下一行，我们用JSON格式来表示其数据结构。在Hive下访问的格式为：

```json
{
    "name": "songsong",
    "friends": ["bingbing" , "lili"] ,       //列表Array, 
    "children": {                      //键值Map,
        "xiao song": 18 ,
        "xiaoxiao song": 19
    }
    "address": {                      //结构Struct,
        "street": "hui long guan" ,
        "city": "beijing" 
    }
}

```

2. 基于上述数据结构，我们在Hive里创建对应的表，并导入数据。
3. 创建本地测试文件test.txt

```shell
songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long guan_beijing
yangyang,caicai_susu,xiao yang:18_xiaoxiao yang:19,chao yang_beijing
```

注意：MAP，STRUCT和ARRAY里的元素间关系都可以用同一个字符表示，这里用 "-"。

4. Hive上创建测试表test

```sql
create table test(
    name string,
    friends array<string>,
    children map<string, int>,
    address struct<street:string, city:string>
)
row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
lines terminated by '\n';
```

字段解释：

- row format delimited fields terminated by ','   -- 列分隔符

- collection items terminated by '_'             -- MAP STRUCT 和 ARRAY 的分隔符(数据分割符号)

- map keys terminated by ':'                    -- MAP中的key与value的分隔符

- lines terminated by '\n';                     -- 行分隔符

5. 导入文本数据到测试表

```shell
hive> load data local inpath '/opt/module/apache-hive-2.3.7/datas/test.txt' into table test
```

#### 3.3 类型转化

Hive的原子数据类型是可以进行隐式转换的，类似于Java的类型转换，例如某表达式使用INT类型，TINYINT会自动转换为INT类型，但是Hive不会进行反向转化，例如，某表达式使用TINYINT类型，INT不会自动转换为TINYINT类型，它会返回错误，除非使用CAST操作。

##### 隐式类型转换规则如下

1. 任何整数类型都可以隐式地转换为一个范围更广的类型，如TINYINT可以转换成INT，INT可以转换成BIGINT。
2. 所有整数类型、FLOAT和STRING类型都可以隐式地转换成DOUBLE。
3. TINYINT、SMALLINT、INT都可以转换为FLOAT。
4. BOOLEAN类型不可以转换为任何其它的类型。

##### 可以使用CAST操作显示进行数据类型转换

例如CAST('1' AS INT)将把字符串'1' 转换成整数1；如果强制类型转换失败，如执行CAST('X' AS INT)，表达式返回空值 NULL。

```shell
hive> select '1'+2, cast('1'as int) + 2;
OK
3.0	3
```

