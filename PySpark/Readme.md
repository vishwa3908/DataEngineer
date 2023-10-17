
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
