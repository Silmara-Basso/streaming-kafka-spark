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
""" docker exec sil-pyspark-master spark-submit --jars data/sqlite-jdbc-3.44.1.0.jar --deploy-mode client ./apps/load_json.py"""

docker-compose -f docker-compose.yml up -d --scale spark-worker=2