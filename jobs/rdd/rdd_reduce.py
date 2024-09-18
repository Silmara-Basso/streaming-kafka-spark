# RDD MapReduce

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD MapReduce:\n')

spark = SparkSession.builder.appName('reduce').getOrCreate()

rdd = spark.sparkContext.textFile("/opt/spark/data/dataset1.txt")

print('\n----------')

# Collect and print all elements of the RDD
for element in rdd.collect():
    print(element)

# Use flatMap to split each line of the RDD into words and flatten them into a single word RDD
rdd2 = rdd.flatMap(lambda x: x.split(" "))

print('\n----------')

# Collect and print all words from the resulting RDD
for element in rdd2.collect():
    print(element)

# Map each word to a tuple (word, 1), preparing for counting
rdd3 = rdd2.map(lambda x: (x,1))

print('\n----------')

# Collect and print all tuples from the RDD
for element in rdd3.collect():
    print(element)

# Apply reduceByKey to add the values ​​of each key (word), performing the word count
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)

print('\n----------')

# Collect and print the word count result
for element in rdd4.collect():
    print(element)

# Map each tuple to reverse order, putting the count before the word, and then sort by the number of occurrences
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()

print('\n----------')

for element in rdd5.collect():
    print(element)

# Filter the tuples, keeping only those whose word contains the letter 'a'
rdd6 = rdd5.filter(lambda x : 'a' in x[1])

print('\n----------')
for element in rdd6.collect():
    print(element)
