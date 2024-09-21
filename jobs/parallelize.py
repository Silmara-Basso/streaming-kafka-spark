# Parallelize

import os
import pyspark
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nParallelize:\n')

spark = SparkSession.builder.appName('parallelize').getOrCreate()

# Creating an RDD (Resilient Distributed Dataset) in Spark from a list of numbers
rdd = spark.sparkContext.parallelize([1,2,3,4,5])

print("Number of Partitions: " + str(rdd.getNumPartitions()))
print("First Element of RDD: " + str(rdd.first()))

# Collecting the data from the RDD to the driver memory (be careful when using it on large RDDs!)
rddCollect = rdd.collect()

print("All Elements of RDD: ") 
print(rddCollect)

# Creating an empty RDD using Spark context
emptyRDD = spark.sparkContext.emptyRDD()

# Creating another empty RDD, reassigning 'rdd' to a new empty RDD
emptyRDD2 = rdd = spark.sparkContext.parallelize([])

print("Is the created RDD empty? " + str(emptyRDD.isEmpty()))
print("Is the created RDD empty? " + str(emptyRDD2.isEmpty()))


