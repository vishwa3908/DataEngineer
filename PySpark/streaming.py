from pyspark.sql import SparkSession
from pyspark.sql.types import *

def process(df):
    df.show()

if __name__ == "__main__":
    print("Starting Application")
    spark = SparkSession.builder.appName('streaming').getOrCreate()

    input_schema = StructType([
        StructField("name",StringType(),True),
        StructField("age",IntegerType(),True)
    ])

    stream_df = spark.readStream.format("csv").option('header','True').schema(input_schema).load(path="C:/Users/Infoobjects_/Desktop/DataEngineer/PySpark/csv/")
    stream_df.printSchema()


    stream_df.query = stream_df.writeStream.foreachBatch(lambda batchdf,batchid:process(batchdf)).queryName("test").start().awaitTermination()