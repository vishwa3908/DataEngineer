**Start Spark Session**

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('app').getOrCreate()
