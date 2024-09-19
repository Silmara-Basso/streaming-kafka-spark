# Repartition
# Add files in dados/lab_partition1, coallesce2 and repartition
import os
from pyspark.sql import SparkSession

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRepartition:\n')

spark = SparkSession.builder.appName('Repartition').getOrCreate()

df = spark.range(0,20)

# Print the number of partitions of the RDD underlying the DataFrame
print("Default Number of Partitions Setting by Spark: " + str(df.rdd.getNumPartitions()))

# Sets Spark's shuffle configuration to use up to 500 partitions
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Creates an RDD with a single element, a tuple (0, 20)
rdd = spark.sparkContext.parallelize((0,20))

# Prints the number of partitions of the RDD, created with the default configuration for the Spark context
print("Number of Partitions After Modifying Spark Parameter: " + str(rdd.getNumPartitions()))

# Creates an RDD with a single element, a tuple (0, 25), specifying 6 partitions
rdd1 = spark.sparkContext.parallelize((0,25), 6)

#Print the number of partitions of the RDD
print("Number of Partitions in Parallelize: " + str(rdd1.getNumPartitions()))

# Saves the RDD to a text file in the specified directory

rdd1.saveAsTextFile("/opt/spark/data/lab_partition1")

# Repartition RDD1 into 4 partitions using repartition method
rdd2 = rdd1.repartition(4)
print("Number of Partitions After Repartitioning: " + str(rdd2.getNumPartitions()))
rdd2.saveAsTextFile("/opt/spark/data/repartition")

# Coalesce RDD1 into 4 partitions, which potentially reduces the number of partitions without causing a full shuffle
rdd3 = rdd1.coalesce(4)
print("Number of Partitions After Repartitioning with Coalesce: "  + str(rdd3.getNumPartitions()))
rdd3.saveAsTextFile("/opt/spark/data/coalesce2")

# Repartitioning in Apache Spark is a technique used to modify the distribution of data across different 
# nodes in a cluster. This technique is important for optimizing the performance of operations that involve data shuffling, 
# such as grouping (groupBy), joins, and sorts (sortBy). Repartitioning an RDD or a DataFrame can help 
# improve the efficiency of parallel processing and manage the workload across nodes.


df = spark.read.option("header",True).csv("/opt/spark/data/dataset2.csv")

df1 = df.repartition(3)

print("Number of df1 Partitions: " + str(df1.rdd.getNumPartitions()))

df1.write.option("header",True).mode("overwrite").csv("/opt/spark/data/zipcodes-state")

df2 = df.repartition(3,"state")

print("Number of df2 Partitions: " + str(df2.rdd.getNumPartitions()))

df2.write.option("header",True).mode("overwrite").csv("/opt/spark/data/zipcodes-state-3state")

df3 = df.repartition("state")

print("Number of df3 Partitions: " + str(df3.rdd.getNumPartitions()))

df3.write.option("header",True).mode("overwrite").csv("/opt/spark/data/zipcodes-state-allstate")

