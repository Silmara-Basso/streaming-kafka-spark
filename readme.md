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