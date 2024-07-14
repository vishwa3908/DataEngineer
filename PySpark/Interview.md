# Pysaprk vs Scala

**Spark with Python**

**Advantages:**

**Ease of Use** : Python is generally considered easier to learn and has a more straightforward syntax compared to Scala.

 **Rich Ecosystem** : Python has a vast ecosystem of libraries for data manipulation, machine learning, and visualization (e.g., Pandas, NumPy, Matplotlib), making it convenient for data scientists.

**Interactive Data Exploration** : Tools like Jupyter Notebooks integrate well with Python, allowing for interactive data exploration and prototyping.

**Considerations:**

    **Performance** : Python can be slower than Scala due to its interpreted nature, especially for CPU-bound tasks. However, for I/O-bound tasks, this difference may be less noticeable.

**
    Concurrency** : Python’s Global Interpreter Lock (GIL) can limit concurrency in multi-threaded scenarios, although Spark’s parallel processing largely mitigates this.

**Spark with Scala**

**Advantages:**

**Performance** : Scala generally performs better than Python for CPU-bound tasks due to its compiled nature and static typing.

**Type Safety** : Scala’s static typing can catch errors at compile-time, reducing bugs in large-scale applications.

 **Functional Programming** : Scala’s functional programming features (e.g., immutability, higher-order functions) can lead to more concise and maintainable code.

**Considerations:**

 **Learning Curve** : Scala might have a steeper learning curve, especially for those unfamiliar with functional programming or static typing.

**Ecosystem** : While Scala has a strong ecosystem, it might not be as extensive or mature as Python’s for certain specialized libraries (e.g., machine learning).

**Choosing Between Them**

 **Team Expertise** : Consider your team’s existing skills and preferences. If your team is more comfortable with Python or has a strong background in data science, Python might be preferable. If your team has experience with Scala or prefers strong typing and performance, Scala could be a better fit.

**Performance Requirements** : If your application requires high performance for CPU-intensive tasks, Scala might be more suitable. However, for many data processing tasks, the performance difference between Python and Scala in Spark might be negligible due to Spark’s distributed nature.

**Integration** : Consider how Spark integrates with other parts of your stack. Both Python and Scala have good integration with Spark, but your existing tooling and infrastructure might influence your choice.

# Pandas vs Pyspark

Key Differences
Scale:

Pandas: Best suited for small to medium-sized datasets that fit into memory.
PySpark: Designed for large-scale datasets and distributed computing.
Performance:

Pandas: Fast for in-memory operations but limited by the memory of a single machine.
PySpark: Optimized for distributed processing and can handle much larger datasets efficiently.
Complexity:

Pandas: Simple and easy to use, making it great for quick data manipulation and analysis.
PySpark: More complex setup and operations due to its distributed nature but powerful for large-scale data processing.
Execution Model:

Pandas: Operates on a single machine and uses in-memory computation.
PySpark: Uses a distributed execution model with lazy evaluation, where transformations are recorded but not executed until an action is called.
Integration with Big Data Ecosystem:

Pandas: Limited integration with big data tools and frameworks.
PySpark: Integrates seamlessly with other big data tools like Hadoop, Hive, and HDFS.

# Architecture

## Overview

**1.Driver** :

 **Definition** : The driver is the main control process that coordinates all the Spark jobs and tasks. It runs the user’s main function and creates the SparkContext.

**Responsibilities** :

* Translates the user program into tasks.
* Distributes these tasks to executors.
* Collects and aggregates results from executors.
* Handles job scheduling and fault tolerance.

**2.** **Cluster Manager** :

 **Definition** : This component allocates resources to the Spark applications. Spark can run on different cluster managers like YARN, Mesos, or its own built-in standalone cluster manager.

**Responsibilities** :

* Manages the cluster’s resources.
* Schedules and monitors Spark jobs.

**3.Executors** :

 **Definition** : Executors are worker processes launched on the worker nodes of the cluster. Each application has its own set of executors.

 **Responsibilities** :

* Executes the tasks assigned by the driver.
* Stores data in memory or on disk.
* Reports the status and results of tasks back to the driver.

## Working

1. Submitting a Spark Application:

   •   A user submits a Spark application, which includes the main program, to the cluster manager.
2. Driver Program Initialization:

   •   The driver program initializes a SparkSession or SparkContext, which acts as the entry point to Spark functionality.
3. Job Execution Plan:

   •   The driver breaks down the Spark application into a directed acyclic graph (DAG) of stages.

   •   Each stage is further divided into smaller tasks based on data partitions.
4. Task Scheduling:

   •   The driver requests resources from the cluster manager to run the tasks.

   •   The cluster manager allocates resources, and the driver distributes the tasks to the executors.
5. Task Execution:

   •   Executors execute the assigned tasks, performing operations on the data.

   •   Tasks read input data, apply transformations (like map, filter, reduce), and produce output.
6. Data Storage:

   •   Executors can store intermediate data in memory or on disk for caching purposes, which helps speed up future operations on the same data.
7. Result Collection:

   •   Once tasks are completed, the executors send the results back to the driver.

   •   The driver collects and aggregates these results, completing the job.
8. Job Completion:

   •   The driver program terminates, and resources are released back to the cluster manager.

# Resource allocation

1.How many executors will you assign for a 10 gb file in hdfs ?

2.How many cores are needed for each executor and memory for each executor ?

Approach for 1 -

1. find number of partitions.  					10gb = 128 partition size = 80 partitions
2. find cpu cores for maximum parallelism			80 cores for partitions
3. find maximum allowed cpu cores for each executor 	5 cores per executor for yarn
4. number of executors = total cores / execuor cores	80/5 = 16 executors

example

1. to process 25 gb of data
   1. how many cpu cores ?
   2. how many executors ?
   3. how much each executor memory ?
   4. total memory

partitions = 25*1024/128 = 200

cpu cores = 200

avg cpu core per executor = 4

executors = 200/4 = 50

each core memory = minm 4 *128 mb

each executor memory = 4x128x4 = 2gb

total memory required =  total executors x memory of each = 50*2 = 100gb

drive memory = generally 2x each executor = 4 gb

# Streaming

# Optimization

## Data Skew

when one partition has significantly more data than other partitions

### Bucketing & Salting

Bucketing
   Key Features of Bucketing

    1.	Fixed Number of Buckets: Data is divided into a fixed number of buckets.
	2.	Column-based Partitioning: Bucketing is done based on the values of specified columns, often referred to as the bucketing columns.
	3.	Consistent Distribution: The bucketing algorithm ensures that the data is evenly distributed across the buckets.

Benefits of Bucketing

    1.	Efficient Joins: When joining two tables bucketed on the same columns and with the same number of buckets, only corresponding buckets need to be joined, reducing the amount of data to process.
	2.	Improved Query Performance: By reducing the data scanned during queries, bucketing can significantly enhance query performance.
	3.	Optimized Aggregations: Aggregations on the bucketing columns can be more efficient since data is pre-organized.

   Disadvantages of Bucketing

    1.	Inflexibility: Once data is bucketed, changing the number of buckets or the bucketing columns requires reprocessing and rewriting the entire dataset.
	2.	Overhead: Managing and maintaining buckets adds complexity to the data processing pipeline. Incorrect configuration can lead to inefficiencies and data skew.
	3.	Storage: Bucketing creates multiple files (buckets) on disk, which can lead to a large number of small files, especially if the number of buckets is high.
	4.	Query Optimization Dependency: Bucketing relies on query optimizers to take advantage of the bucketed structure. If the optimizer does not leverage bucketing, the benefits are not realized.
	5.	Resource Utilization: Poorly chosen bucket sizes can result in uneven resource utilization, leading to some tasks being overloaded while others are underutilized.

   Use Cases for Bucketing

    1.	Frequent Joins: When two large tables are frequently joined on the same column, bucketing can significantly speed up the join operation by reducing the amount of data scanned and processed.
	•	Example: Joining a user transaction table and a user details table on the user ID column.

# ELT VS ETL

Key Differences
Order of Operations:

ETL: Transformations happen before loading into the data warehouse.
ELT: Transformations happen after loading into the data warehouse.
Transformation Location:

ETL: Transformations are done outside the target system, often using separate ETL tools.
ELT: Transformations are done inside the target system, leveraging its processing power.
Scalability:

ETL: Can be limited by the capacity of the ETL tools and intermediate storage.
ELT: Can leverage the scalability of modern cloud data warehouses and data lakes.
Data Latency:

ETL: Typically used for batch processing, which can introduce latency.
ELT: Can support both batch and real-time processing, reducing data latency.
Flexibility:

ETL: Less flexible as data transformations are predefined and must be done before loading.
ELT: More flexible as raw data is available for different transformations as needed.

Example Scenarios
   ETL Scenario
      Scenario: A retail company needs to generate daily sales reports by consolidating data from various transactional systems.

      Implementation:

      Extract sales data from transactional databases.
      Transform data to aggregate sales by product, region, and time period.
      Load the transformed data into a data warehouse for reporting.
   ELT Scenario
      Scenario: A tech company collects large amounts of log data from various applications and needs to perform ad-hoc analyses.

      Implementation:

      Extract log data from application servers.
      Load raw log data into a cloud data warehouse like BigQuery.
      Transform and analyze data directly within BigQuery to identify trends and issues.
      Choosing Between ETL and ELT
      Use ETL when you need to ensure data quality and consistency before loading, have compliance requirements, or are working with legacy systems.
      Use ELT when working with large datasets, using modern cloud data warehouses, needing flexibility for ad-hoc analyses, or aiming for real-time data processing.
      The choice between ETL and ELT depends on your specific data processing needs, the tools and systems you are using, and your performance and scalability requirements.






