# PySpark

A Project to practice all pyspark functionalities

## Authors

- [@VishwaranjanKumar](https://github.com/vishwa3908)

## Start Spark Session

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('app').getOrCreate()

## Read Files

The header and schema are separate things.

Header:

If the csv file have a header (column names in the first row) then set header=true. This will use the first row in the csv file as the dataframe's column names. Setting header=false (default option) will result in a dataframe with default column names: _c0, _c1, _c2, etc.

    df = spark.read.csv("",header=True)

    df = spark.read.option('header','true').csv("")

    df = spark.read.format("csv").load("")

## Print Schema of df

    df.printSchema()

## Display df

    df.head()
    df.tail()
    df.show()

## Display Columns

    df.columns

## Selecting Coloumn

#### Single Column

    df.select("").show()

#### Multiple Columns

    df.select(["",""]).show()

## Adding New Column

    * not a inplace function
    * if new column name is same as already existing column override will happen

    df.withColumn('index',df['status']+2)

## Drop Column

Not an Inplace Operator

#### Drop Single Column

    df.drop('')

#### Drop Multiple Column

    df.drop('','')
    df.drop(col(''),'')
    df.drop(col(''),col(''))

## Rename Columns

Not an Inplace Operator

    df.withColumnRenamed('old','new')

## Handling Missing Values

#### Dropping Rows with Null Values

    df.na.drop(how='any/all')

all - if all data in a row is Null

any - if any row contains a null data

#### Drop Rows with NULL Values on Selected Columns

    df.na.drop(how='any/all',subset=['',''])

#### Thresh

thresh - If non NULL values of particular row or column is less than thresh value then drop that row or column.

    df.na.drop(how='any/all',thresh=3,subset=['',''])

## UDF

USER DEFINED FUNCTION

#### FIRST WAY

    def total(s,b):
        return s+b
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import udf

    tl = udf(lambda s,b:total(s,b),IntegerType())
    df.withColumn('tp',tl(df.s,df.b))

#### SECOND WAY

    @udf(return)

# Transformation

## concat

    df = df.withColumn("FullName",concat(df.first_name,df.last_name))

## concat_ws

    df = df.withColumn("FullName",concat_ws(" ",df.first_name,df.last_name))

## concat vs concat_ws

Both CONCAT() and CONCAT_WS() functions are used to concatenate two or more strings but the basic difference between them is that CONCAT_WS() function can do the concatenation along with a separator between strings, whereas in CONCAT() function there is no concept of the separator. Other significance difference between them is that CONCAT()function returns NULL if any of the argument is NULL, whereas CONCAT_WS() function returns NULL if the separator is NULL.

## when & otherwise

when() function take 2 parameters, first param takes a condition and second takes a literal value or Column, if condition evaluates to true then it returns a value from second param

    df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
