# PySpark and Apache Kafka for Batch and Streaming Data Processing
## Preparation of the Work Environment with Python and PySpark
## PySpark Cluster Configuration

### Create and Initialize the Cluster
docker-compose -f docker-compose.yml up -d --scale spark-worker=2

### Spark Master
http://localhost:9091

### History Server
http://localhost:18081

### Lab1 - Load Json file
 docker exec sil-pyspark-master spark-submit --jars data/sqlite-jdbc-3.44.1.0.jar --deploy-mode client ./apps/load_json.py

### Lab2 - Execute log level for errors only (I'm using the log4j2.properties in conf directory to configure the log level)
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/log_level.py

### Lab3 - Execute dedup
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/dedup.py

### Lab4 - Execute add columns
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/add_new_col.py

### Lab5 - Date Operations examples
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/add_month.py

### Lab6 - Using Spark Expr to query data
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/expr.py

### Lab7 - Working with Spark arrays
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/arrays.py

### Lab8 - Working with Dictionary
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/Dictionary.py

### Lab9 - Selecting firsts and lasts rows
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/top_rows.py

### Lab10 - Selecting a sample
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/sampling.py

### Lab11 - Working with rows
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/row.py

### Lab12 - Aggregate e Stats 
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/aggregate_stats.py

### Lab13 - Convert Map to Column
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/map_to_columns.py

### Lab14 - Convert Column to Map
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/columns_to_map.py

### Lab15 - Filter
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/filter.py

### Lab16 - Drop Columns
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/drop_column.py

### Lab17 - Explode Function
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/explode.py

### Lab18 - Loop 
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/loop.py

### Lab19 - Pivot 
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/pivot.py

### Lab20 - Split Function
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/split.py

### Lab21 - Collect
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/collect.py

### Lab22 - Operators
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/operators.py

### Lab23 - With Column 
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/withcolumn.py

### Lab24 - Column Operations
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/column_operations.py

### Lab25 - maptype - dataframe column
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/maptype-dataframe-column.py

### Lab26 - rdd
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_actions.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_broadcast.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_map.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd-flatMap.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_dataframe.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_reduceByKey.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_reduce.py

docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/rdd/rdd_reduce2.py

### Lab27 - Partition By
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/partitionby.py

### Lab28 - Mappartition
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/mappartitions.py

### Lab29 - Re-partition
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/repartition.py

### Lab30 - Range partition
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/range_partition.py

### Lab31 - Dataframe repartition
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/df_repartition.py