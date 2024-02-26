# Data Catalog

* Persistent central metadata repository
* metadata repository is the database of descriptive information about data
* descriptive information of data includes **schema**,location,data types, business relevant info

## What makes AWS Glue Data Catalog

1. Database -  A logical group of tables
   * Tables -  metadata definition
2. Crawlers and Classifiers -  Detect and infer schemas to store it in Data catalog
3. connections - An object that contains the properties to conncet to a particular data store
4. AWS Glue Schema Registry - schema and registry for streaming data

### AWS Glue Crawlers & Classifiers

1. create and update Data Catalog tables
2. use custom/built-in classifiers to infer and determine structure of data
3. can crawl file based & table based data stores
4. can detect changes to existing data , structure and update the table definition
5. will not be able to determine the relationship
6. can run on schedule, by trigger and on demand
7. curently glue does not support crawlers on data stream

When to use Data Catalog

1. need of unified view of data
2. keep track of data
3. ETL
4. query,analyze data


# ETL
