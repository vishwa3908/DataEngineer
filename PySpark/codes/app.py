from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
spark = SparkSession.builder.appName('app').getOrCreate()

sc = spark.sparkContext
print(sc)
rdd = sc.parallelize([1,2,3,4])
print(rdd.collect())
df_1 = spark.read.csv("machine-readable-business-employment-data-jun-2023-quarter.csv",header=True,inferSchema=True)


df_1 = df_1.withColumn('test',lit(None)).withColumn("vk",lit(None))
df_1.show(2)

