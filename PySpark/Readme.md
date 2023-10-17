
# PySpark

A Project to practice all pyspark functionalities




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








