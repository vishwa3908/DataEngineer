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

```python
df = spark.read.csv("",header=True)  
df = spark.read.option('header','true').csv("")  
df = spark.read.format("csv").load("")
```

## Create Schema

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

## Selecting Coloumn

#### Single Column

```python
df.select("").show()
```

#### Multiple Columns

```python
df.select(["",""]).show()
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
```

### createOrReplaceGlobalTempView()

It is used to create temp views or tables globally , when can be accessed across the sessions with in spark applications

to query these table , we need append **global_temp.<table_name>**

```python
changed_df.createOrReplaceGlobalTempView('name')
%sql
SELECT * FROM global_temp.name;

```

## RDD

RDDs are fault-tolerant, immutable distributed collections of objects, which means once you create an RDD you cannot change it. Each dataset in RDD is divided into logical partitions, which can be computed on different nodes of the cluster.

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
