from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('app').getOrCreate()

sc = spark.sparkContext
print(sc)
rdd = sc.parallelize([1,2,3,4])
print(rdd.collect())
df_1 = spark.read.csv("machine-readable-business-employment-data-jun-2023-quarter.csv",header=True,inferSchema=True)
# print(df_1.show())

print(df_1.columns)
print(df_1.select(["Period","STATUS"]).show())