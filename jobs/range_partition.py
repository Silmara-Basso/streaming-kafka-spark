# Range Partition

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import spark_partition_id

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRange Repartition:\n')

spark = SparkSession.builder.appName('RangePart').getOrCreate()

data_sample = [(1,10),(2,20),(3,10),(4,20),(5,10),
             (6,30),(7,50),(8,50),(9,50),(10,30),
             (11,10),(12,10),(13,40),(14,40),(15,40),
             (16,40),(17,50),(18,10),(19,40),(20,40)]

# Creating a DataFrame in Spark from data, with 'id' and 'value' columns
df = spark.createDataFrame(data_sample, ["id", "value"])


# Repartitioning the DataFrame based on 3 partitions and the 'value' column,
# writing the result in CSV format with header, overwriting existing data
df.repartition(3, "value") \
  .write.option("header", True) \
  .mode("overwrite") \
  .csv("/opt/spark/data/re-partition")

# Splitting the DataFrame into 3 partitions by range of values ​​in the 'value' column, writing the result
# in CSV format with header, overwriting existing data
df.repartitionByRange(3, "value") \
  .write.option("header", True) \
  .mode("overwrite") \
  .csv("/opt/spark/data/range-partition")

# Explanation of the physical plan for partitioning the DataFrame into 3 partitions by range of values ​​in the 'value' column
df.repartitionByRange(3, "value").explain(True)


# Range Partition in Apache Spark is a data partitioning technique based on column values.
# This allows data to be distributed across partitions based on ranges of values ​​for a specific column.
# This type of partitioning is useful when working with large data sets where balancing workloads across processing nodes is important.

# Creating a DataFrame
df = spark.createDataFrame(data_sample,["id","value"])

# Defining the number of partitions
num_part = 4

# Creating a range-partitioned DataFrame
df_partitioned = df.repartitionByRange(num_part, col("value"))

# Função para exibir as partições
def display_partitions(df):
    df_with_partition_id = df.withColumn("partitionId", spark_partition_id())
    df_with_partition_id.show(truncate=False)
    
# Exibindo o DataFrame particionado com os IDs das partições
display_partitions(df_partitioned)
