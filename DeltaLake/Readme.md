# Why ?

1. Challenges in implementation of data lake
2. Missing ACID properties
3. Lack of schema enforcement
4. lack of consistency
5. lack of data quality
6. too many small files
7. corrupted data due to frequent job failure in prod

# Features

1. Open Format
2. ACID Transactions
3. Schema Enforcement and Evolution
4. Audit History
5. Time Travel
6. Deletes and Upsets
7. Scalable metadata managment
8. Unified batch and streaming source and sink

# Create Delta Table

## By Path

```python
table_df.write \
    .format('delta') \
    .mode('overwrite') \
    .save('/FileStore/tables/delta/')
```

## Metastore

```python
table_df.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('delta_lake.players')

table_df.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('TYPE') \
    .saveAsTable('delta_lake.players_type')
```

# Select Version

if we want to time travel and see previous version of tables


```python
SELECT * FROM delta_lake.players TIMESTAMP  AS OF '2024-03-12T04:42:33.000+0000'

SELECT * FROM delta_lake.players VERSION  AS OF 1
```

# Restore

available only for DML commands, not for drop

```python
restore table Delta_lake.Players version as of 3;
```
