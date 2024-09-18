# RDD FlatMap

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD FlatMap:\n')

spark = SparkSession.builder.appName('flatmap').getOrCreate()

data_sample = ["Data Scientist Training",
               "Data Engineer Training",
               "Data Analyst Training"]

# Creating an RDD from a list of strings
rdd = spark.sparkContext.parallelize(data_sample)

print('\nRDD Original:\n')
for element in rdd.collect():
    print(element)


# Applying a flatMap to split each string into words and flatten the result into a single RDD
rdd2 = rdd.flatMap(lambda x: x.split(" "))

print('\nRDD after flatMap:\n')

# Collecting and printing each word of the resulting RDD
for element in rdd2.collect():
    print(element)



# In Spark, map and flatMap are two transformation operations that apply a function to each element of an RDD, 
# but they work in slightly different ways:

# map: The map operation applies a function to each element of the RDD and returns a new RDD where each element 
# is the result of the applied function. Each input in the original RDD results in exactly one output in the new RDD. 
# For example, if you have an RDD containing numbers [1, 2, 3] and you apply a function that multiplies each 
# number by 2 using map, you will get a new RDD [2, 4, 6]. 

# flatMap: The flatMap operation also applies a function to each element of the RDD, but the function used must 
# return a sequence for each input, and flatMap flattens all sequences into a single RDD. 
# This means that each input can be mapped to zero or more outputs. For example, if you have an RDD with sentences 
# and apply a function that splits each sentence into words, flatMap will result in an RDD with all the words from 
# all the sentences, not separate lists of words for each sentence.


