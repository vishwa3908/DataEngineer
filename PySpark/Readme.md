# PySpark

A Project to practice all pyspark functionalities

## Authors

- [@VishwaranjanKumar](https://github.com/vishwa3908)

## Start Spark Session

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('app').getOrCreate()
```

## Read Files

The header and schema are separate things.

Header:

If the csv file have a header (column names in the first row) then set header=true. This will use the first row in the csv file as the dataframe's column names. Setting header=false (default option) will result in a dataframe with default column names: _c0, _c1, _c2, etc.

3 type of modes

* failfast -> it will fail if there is any datatype mismatch. useful  when need to parse data in desired datatype
* permissive (default) -> if spark is not able to parse it due to datatype mismatch  then make it as a null without impacting the other result.
* dropmalformed -> whatever records have issues, it will drop those records.

```python
df = spark.read.csv("",header=True)  
df = spark.read.option('header','true').csv("")  
df = spark.read.format("csv").load("")
```

## Create Schema

it is not preferred to use inferschema because spark needs to read the data and get the schema and it may be not correct always.

```python
schema = StructType([
    StructField("ProductId",IntegerType(),True),
    StructField("CustomerId",StringType(),True),
    StructField("OrderDate",DateType(),True),
    StructField("Location",StringType(),True),
    StructField("SourceOrder",StringType(),True)]
)

df = spark.read.format('csv').option('inferSchema','true').schema(schema).load("")
```

## Create DataFrame

```python
#method 1
a = [(1,'vishwa'),(2,'Raja'),(3,'Mohan'),(4,'Sita')]
coln = ['id','name']
b = spark.createDataFrame(a).toDF(*coln)
b.show()

#method 2
a = [(1,'vishwa'),(2,'Raja'),(3,'Mohan'),(4,'Sita')]
coln = ['id','name']
b = spark.createDataFrame(a,coln)
b.show()
```

## Print Schema of df

```python
df.printSchema()
```

## Display df

```python
df.head()
df.tail()
df.show()
```

## Display Columns

```python
df.columns
```

## truncate()

to show the whole row data and not ....

set truncate=False

```python
df.show(truncate=False)
```

## Selecting Coloumn

#### Single Column

```python
df.select("").show()
```

#### Multiple Columns

```python
df.select(["",""]).show()
```

## Select Expr

if we want to see some column after calculation

```python
df.select('*',expr("product_price * quantity as subtotal"))
df.selectExpr('*',"product_price * quantity as subtotal")
```

## Adding New Column

### Single Column

    * not a inplace function
    * if new column name is same as already existing column override will happen

    df.withColumn('index',df['status']+2)

### Multiple Columns

```python
df = df.withColumns({'OrderedYear':year(df.OrderDate),'Month':month(df.OrderDate),'Quarter':quarter(df.OrderDate),'DayOfWeek':dayofweek(df.OrderDate)})
```

## Drop Column

Not an Inplace Operator

#### Drop Single Column

```python
df.drop('')
```

#### Drop Multiple Column

```python
df.drop('','')
df.drop(col(''),'')
df.drop(col(''),col(''))
```

## dropDuplicates

```python
df.dropDuplicates()
df.dropDuplicates(['col1','col2'])
```

## Rename Columns

Not an Inplace Operator

```python
df.withColumnRenamed('old','new')
```

## Change Schema of Column

```python
df.withColumn("age",df.age.cast(IntegerType()))
df.withColumn("age",df.age.cast('int'))
df.withColumn("age",df.age.cast('integer'))
```

## Handling Missing Values

#### Dropping Rows with Null Values

```python
df.na.drop(how='any/all')
```

all - if all data in a row is Null

any - if any row contains a null data

#### Drop Rows with NULL Values on Selected Columns

```python
df.na.drop(how='any/all',subset=['',''])
```

#### Thresh

thresh - If non NULL values of particular row or column is less than thresh value then drop that row or column.

```python
df.na.drop(how='any/all',thresh=3,subset=['',''])
```

## UDF

USER DEFINED FUNCTION

#### FIRST WAY

```python
def total(s,b):
    return s+b
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf  
tl = udf(lambda s,b:total(s,b),IntegerType())
df.withColumn('tp',tl(df.s,df.b))
```

#### SECOND WAY

    @udf(return)

## Transformation

### concat

```python
df = df.withColumn("FullName",concat(df.first_name,df.last_name))
```

### concat_ws

```python
df = df.withColumn("FullName",concat_ws(" ",df.first_name,df.last_name))concat vs concat_ws
```

Both CONCAT() and CONCAT_WS() functions are used to concatenate two or more strings but the basic difference between them is that CONCAT_WS() function can do the concatenation along with a separator between strings, whereas in CONCAT() function there is no concept of the separator. Other significance difference between them is that CONCAT()function returns NULL if any of the argument is NULL, whereas CONCAT_WS() function returns NULL if the separator is NULL.

### filter

```python
df.filter('gender = M')
```

### when & otherwise

when() function take 2 parameters, first param takes a condition and second takes a literal value or Column, if condition evaluates to true then it returns a value from second param

```python
df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
```

### groupBy

```python
# Syntax
DataFrame.groupBy(*cols)
#or 
DataFrame.groupby(*cols)
```

When we perform `groupBy()` on PySpark Dataframe, it returns `GroupedData` object which contains below aggregate functions.

`count()` – Use [groupBy() count()](https://sparkbyexamples.com/pyspark/pyspark-groupby-count-explained/) to return the number of rows for each group.

`mean()` – Returns the mean of values for each group.

`max()` – Returns the maximum of values for each group.

`min()` – Returns the minimum of values for each group.

`sum()` – Returns the total for values for each group.

`avg()` – Returns the average for values for each group.

`agg()` – Using [groupBy() agg()](https://sparkbyexamples.com/pyspark/pyspark-groupby-agg-aggregate-explained/) function, we can calculate more than one aggregate at a time.

`pivot()` – This function is used to Pivot the DataFrame which I will not be covered in this article as I already have a dedicated article for[ Pivot &amp; Unpivot DataFrame](https://sparkbyexamples.com/spark/how-to-pivot-table-and-unpivot-a-spark-dataframe/).

```python
df.groupBy("department").sum("salary").show(truncate=False)
sales_df.groupBy(['CustomerId','ProductId','SourceOrder']).sum('ProductPrice').orderBy('CustomerId').show()
```

### Sort and orderBy

```python
df.sort("column1", "column2", ascending=[True, False]) 
df.orderBy(col("department"),col("state")).show(truncate=False)
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
```

### Union vs UnionByName

#### Union

Union is applied on df having same number of columns. if columns are same then it will work perfectly. order of columns must be same

```python
df = df_1.union(df_2)
```

#### UnionByName

it is used  if columns name are different, order of column is different , number of columns may be same or not

```python
df = df_1.unionByName(df_2,allowMissingColumns=True)
```

### Explode

if a column contains list of elements , it will create separate rows for each element

```python
df.withColumn('codingskills',explode(col('skills)))
```

### Map

only for rdd objects

```python
map_data = [('vishwaranjan','kumar'),('Aniket','Yadav')]
map_rdd = spark.sparkContext.parallelize(map_data)
print(map_rdd.collect())
rdd_map = map_rdd.map(lambda x: x + (x[0]+' '+x[1],))
print(rdd_map.collect())
```

if we have dataframe

```python
new_df = spark.createDataFrame(map_data,['First_Name','Last_Name'])
new_df.show()
new_rdd = new_df.rdd.map(lambda x: x+(x[0]+' '+x[1],))
changed_df = new_rdd.toDF(['First_Name','Last_Name','Full_Name'])
changed_df.show()

```

### createOrReplaceTempView()

creates a temporary table within the session ,if already created It will be replaced

```python
changed_df.createOrReplaceTempView('name')
df = spark.sql('select * from name')
df = spark.read.table('name')
```

### createOrReplaceGlobalTempView()

It is used to create temp views or tables globally , when can be accessed across the sessions with in spark applications

to query these table , we need append **global_temp.<table_name>**

```python
changed_df.createOrReplaceGlobalTempView('name')
%sql
SELECT * FROM global_temp.name;

```

## Cache and Persist

we should cache  a dataframe which has to be reused multiple time

we should not cache big dataframes because it will not fit in memory and hinder performance

cache is lazy

cache ->   rdd -> in memory (default)

    dataframes -> in memory and disk (default)

whatever can fit in memory will fit in memory and laters will be fit in disk

**persist**

storage level can be changed by setting optional parameters.



# RDD

RDDs are fault-tolerant, immutable distributed collections of objects, which means once you create an RDD you cannot change it. Each dataset in RDD is divided into logical partitions, which can be computed on different nodes of the cluster.

### RDD Operations

#### RDD Transformations

[Spark RDD Transformations](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/) are lazy operations meaning they don`t execute until you call an action on RDD. Since RDDs are immutable, When you run a transformation(for example map()), instead of updating a current RDD, it returns a new RDD.

Some transformations on RDDs are `flatMap()`, `map()`, `reduceByKey()`, `filter()`, `sortByKey()` and all these return a new RDD instead of updating the current.

1. Narrow
2. Wide

**Narrow** :- Narrow transformations are the result of [map()](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/#rdd-map) and [filter()](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/#rdd-filter) functions and these compute data that live on a single partition meaning there will not be any data movement between partitions to execute narrow transformations.Functions such as `map()`, `mapPartition()`, `flatMap()`, `filter()`, `union()` are some examples of narrow transformation

---

**Wide**:- Wide transformations are the result of *[groupByKey()](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/#rdd-groupbykey)* and *[reduceByKey()](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/#rdd-reducebykey)* functions and these compute data that live on many partitions meaning there will be data movements between partitions to execute wide transformations. Since these shuffles the data, they also called shuffle transformations.

---

Functions such as `groupByKey()`, `aggregateByKey()`, `aggregate()`, `join()`, `repartition()`,distinct() are some examples of a wide transformations.

**Note:** When compared to Narrow transformations, wide transformations are expensive operations due to shuffling.

Number of stages depend upon number of wide transformations

***No. of stages = no. of wide transformations + 1***

---

#### RDD Actions

[RDD Action operation](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-actions/) returns the values from an RDD to a driver node. In other words, any RDD function that returns non RDD[T] is considered as an action. RDD operations trigger the computation and return RDD in a List to the driver program.

Some actions on RDDs are `count()`,  `collect()`,  `first()`,  `max()`,  `reduce()`  and more.

***no. of jobs = no. of actions***

***no. of tasks = no. of partitions***

### RDD creation

#### sparkContext.parallelize()

sparkContext.parallelize is used to parallelize an existing collection in your driver program. This is a basic method to create RDD.

```python
data = [('Name','Age'),('Vishwa',23),('Aniket',23),('Ankita',20)]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
columns = rdd.first()
rdd = rdd.filter(lambda x : x!= columns)
rdd.toDF(columns)
```

#### sparkContext.textFile()

Using textFile() method we can read a text (.txt) file from many sources like HDFS, S#, Azure, local e.t.c into RDD.

```python
rdd = spark.sparkContext.textFile("/FileStore/tables/sales_csv.txt")
```

#### reduce()

reduce is an action.output is a single value.

#### reduceByKey()

Spark RDD `reduceByKey()` transformation is used to merge the values of each key using an associative reduce function.  It is a wider transformation as it shuffles data across multiple partitions and **it operates on pair RDD (key/value pair).**

```python
closed_orders_rdd = test_rdd.filter(lambda x: x[3]=='closed')
closed_orders_rdd = closed_orders_rdd.map(lambda x:(x[2],1))
closed_orders = closed_orders_rdd.reduceByKey(lambda x,y:x+y)
```

#### groupByKey()

[https://sparkbyexamples.com/spark/spark-groupbykey-vs-reducebykey/](https://sparkbyexamples.com/spark/spark-groupbykey-vs-reducebykey/)

[https://medium.com/@sujathamudadla1213/explain-the-difference-between-groupbykey-and-reducebykey-bf0171e985ac](https://medium.com/@sujathamudadla1213/explain-the-difference-between-groupbykey-and-reducebykey-bf0171e985ac)

#### countByValue

Same as reduced by key but it is an action and not transformations. no further transformation operations can be done

```python
test_rdd.filter(lambda x:x[3]=='closed'). \
    map(lambda x:(x[2])). \
        countByValue()
```

#### reduceByKey() vs countByValue()

| reduceByKey()                                                         | countByValue()                                                  |
| --------------------------------------------------------------------- | --------------------------------------------------------------- |
| returns an rdd which can be further<br />used in a distributed manner | return a map which can not be<br /> used in distributed manner. |
| RDD Transformation                                                    | RDD action                                                      |

#### Join

costly operation because it requires lot of reshuffling

#### broadcast

only datasets can be broadcasted . To broadcast a rdd you need to add collect

```python
broadcasted_rdd = sc.broadcast(join1_rdd.collect())
```

#### repartition()

change the number of partitions(either increase or decrease) . It will do complete reshuffling of data.partitions tends to have same amount of data

increase -> for more parallelism

decrease->  eg, in case of filter

#### coalesce()

It can only decrease the number of partitions.its intent is to avoid reshuffling. partitions can have uneven amount of data.

| Repartition                                                        |                      Coalesce                      |
| ------------------------------------------------------------------ | :------------------------------------------------: |
| change the number of partitions<br />(either increase or decrease) | It can only decrease the<br />number of partitions |
| It will do complete reshuffling of data                            |         its intent is to avoid reshuffling         |
| partitions tends to have same<br /> amount of data                 |  partitions can have uneven<br />amount of data.  |
| mainly used to increase partitions                                 |      used for decreasing number of parttions      |

#### cache()

# SparkSQL

managed table vs external table

## Disadvantages of MapReduce()

1. Hard to write the code
2. confined to map and reduce
3. write to disk

spark is alternate of mapreduce

**spark is general purpose in memory compute engine**
