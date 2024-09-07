# PySpark and Apache Kafka for Batch and Streaming Data Processing
## Preparation of the Work Environment with Python and PySpark
## PySpark Cluster Configuration

### Create and Initialize the Cluster
docker-compose -f docker-compose.yml up -d --scale spark-worker=2

### Spark Master
http://localhost:9091

### History Server
http://localhost:18081

### Para executar a carga do arquivo Json
 docker exec sil-pyspark-master spark-submit --jars data/sqlite-jdbc-3.44.1.0.jar --deploy-mode client ./apps/load_json.py

### To execute log level test (I'm using the log4j2.properties in conf directory to configure the log level)
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/log_level.py

### To execute dedup test
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/dedup.py

### To execute add columns test
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/add_new_col.py

### To Date Operations examples
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/add_month.py

### To Date Operations examples
docker exec sil-pyspark-master spark-submit --deploy-mode client ./apps/expr.py