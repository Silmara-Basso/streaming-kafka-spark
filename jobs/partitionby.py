# Partition
# This  lab will create the partition1, artition2, artition3, artition4 folders in Dados or overwrite if exists
 
import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nPartition:\n')

spark = SparkSession.builder.appName('Partition').getOrCreate()

# Reads a CSV file with header and stores it in a DataFrame
df_lab = spark.read.option("header",True).csv("/opt/spark/data/dataset2.csv")
df_lab.show()

# Print the number of partitions of the RDD underlying the DataFrame
# See details at the end of the script
print('\nOriginal Number of Partitions:')
print(df_lab.rdd.getNumPartitions())

# Write the DataFrame to CSV files, partitioned by state, with header
# and overwriting existing files
df_lab.write.option("header",True) \
        .partitionBy("State") \
        .mode("overwrite") \
        .csv("/opt/spark/data/partition1")
        
# Write the DataFrame to CSV files, partitioned by state and city, with header
# and overwriting existing files
df_lab.write.option("header",True) \
        .partitionBy("state","city") \
        .mode("overwrite") \
        .csv("/opt/spark/data/partition2")

# Repartition the DataFrame into 2 partitions
df_lab = df_lab.repartition(2)

# Print the number of partitions after repartitioning
print('\nNúmero de Partições Após o Reparticionamento:')
print(df_lab.rdd.getNumPartitions())

# Rewrite the DataFrame to CSV files, partitioned by state, with header
# and overwriting existing files, after repartition
df_lab.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/opt/spark/data/partition3")
        
#Reads DataFrame from state-partitioned CSV files, with header
df_Partition = spark.read.option("header",True).csv("/opt/spark/data/partition1")
df_Partition.printSchema()

# Lê um único arquivo CSV específico, de uma partição específica de estado e cidade, com cabeçalho
df_SinglePart = spark.read.option("header",True).csv("/opt/spark/data/partition2/state=SC/city=BLUMENAU")
df_SinglePart.printSchema()
df_SinglePart.show()

# Read data from the DataFrame
parqdf = spark.read.option("header",True).csv("/opt/spark/data/partition1")

# Create a temporary view of the DataFrame for use with SQL
parqdf.createOrReplaceTempView("LABTABLE")

# Executes a SQL query on the temporary view, filtering by specific state and city, and displays the results
# Executes a SQL query on the temporary view, filtering by specific state and city, and displays the results
spark.sql("select * from LABTABLE where State='SC' and City = 'BLUMENAU'").show()

# Write the DataFrame to CSV files, limiting the maximum number of records per file, 
# partitioned by state, with header and overwriting existing files
df_lab.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/opt/spark/data/partition4")


# The getNumPartitions() method is used to get the number of partitions of an RDD in Apache Spark. 
# When applied as df_lab.rdd.getNumPartitions(), it returns the number of partitions of the RDD underlying 
# the df_lab DataFrame. Here is a more detailed explanation:

# DataFrame to RDD: The .rdd attribute converts a DataFrame to its underlying RDD. 
# DataFrames in Spark are built on the concept of RDDs (Resilient Distributed Datasets), 
# which are distributed collections of objects that can be processed in parallel. Each DataFrame has an RDD 
# behind it that manages the distribution and processing of the data.

# Partitions: Partitions are subdivisions of the original dataset, and each partition can be processed in parallel 
# on different nodes of a cluster. The granularity of the partitions directly influences the performance of 
# parallel processing. Fewer partitions can lead to less parallelism and more partitions can increase 
# management overhead, so there is a balance point that usually depends on the size of the dataset 
# and the cluster configuration.

# getNumPartitions(): This method returns the number of partitions the RDD is using. 
# This number can provide insights into how the data is distributed and how processing tasks 
# will be parallelized. An adequate number of partitions helps optimize distributed processing performance.

# So when you call df_lab.rdd.getNumPartitions(), you are investigating how many partitions the RDD 
# behind your DataFrame is using, which can be useful for understanding and optimizing the performance of 
# operations that involve data shuffling, such as grouping and sorting, or for tuning the 
# parallelism configuration of your Spark job.
