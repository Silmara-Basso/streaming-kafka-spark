# Dataframe Partition

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDataframe Partition:\n')

spark = SparkSession.builder.appName('dfRepartition').getOrCreate()

df1 = spark.range(0,20)

print("Number of df1 Partitions: " + str(df1.rdd.getNumPartitions()))

df1.write.mode("overwrite").csv("/opt/spark/data/partition-df1")

df2 = df1.repartition(6)

df2.write.mode("overwrite").csv("/opt/spark/data/partition-df2")

print("Number of df2 Partitions: " + str(df2.rdd.getNumPartitions()))

df3 = df1.coalesce(5)

df3.write.mode("overwrite").csv("/opt/spark/data/partition-df3")

print("Number of df3 Partitions: " + str(df3.rdd.getNumPartitions()))

df4 = df1.groupBy("id").count()

df4.write.mode("overwrite").csv("/opt/spark/data/partition-df4")

print("Number of df4 Partitions: " + str(df4.rdd.getNumPartitions()))


# The coalesce function in Spark is primarily used to reduce the number of partitions in a DataFrame. 
# However, unlike the repartition function, coalesce cannot increase the number of partitions to 
# more than what already exists. If you try to increase the number of partitions using coalesce, it will simply 
# return the original DataFrame without making any changes.

# Here is the difference between coalesce and repartition:

# coalesce(numPartitions): Reduces the number of partitions to numPartitions. 
# This function is optimized to reduce the number of partitions and avoids excessive data movement 
# between existing partitions. It cannot increase the number of partitions.

# repartition(numPartitions): Reorganizes the DataFrame into numPartitions, moving data between partitions
# as needed. You can either increase or decrease the number of partitions.
