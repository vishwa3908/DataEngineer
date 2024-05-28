from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .appName('stream') \
    .getOrCreate()

# Define schema for streaming data
stream_schema = StructType([
    StructField("ID", IntegerType(), nullable=True),
    StructField("Name", StringType(), nullable=True),
    StructField("Address", StringType(), nullable=True),
    StructField("Birth Date", StringType(), nullable=True),
    StructField("City", StringType(), nullable=True),
    StructField("Created At", TimestampType(), nullable=True),
    StructField("Email", StringType(), nullable=True),
    StructField("Latitude", DoubleType(), nullable=True),
    StructField("Longitude", DoubleType(), nullable=True),
    StructField("Password", StringType(), nullable=True),
    StructField("Source", StringType(), nullable=True),
    StructField("State", StringType(), nullable=True),
    StructField("Zip", IntegerType(), nullable=True)
])

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'pysparkproject'

# Read streaming data from Kafka
stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', kafka_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load()


# Define transformation function for each batch
def transform_batch(batch_df,batch_id):
    batch_df = batch_df.withColumn('value',expr('CAST(value AS STRING)')).withColumn('key',expr("CAST(key as STRING)"))
    
    bio_data_df = batch_df.select('value')
    bio_df = bio_data_df.select(from_json(col("value"), stream_schema).alias("data")).select("data.*")
    # bio_df.show(truncate=False)
    bio_df = group_according_to_Source(bio_df)
    bio_df.show(truncate=False)
    return bio_df

def group_according_to_Source(bio_df):
    bio_df = bio_df.groupBy('Source').agg(count('*').alias('total'))
    return bio_df

# Write the decoded DataFrame to the console
query = stream_df.writeStream \
    .foreachBatch(transform_batch) \
    .outputMode('append') \
    .option('checkpointLocation', 'checkpoint') \
    .queryName('stream_output') \
    .start()

# Wait for the query to terminate
query.awaitTermination()
