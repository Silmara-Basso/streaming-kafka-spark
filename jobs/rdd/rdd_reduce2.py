#RDD MapReduce

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD MapReduce:\n')

spark = SparkSession.builder.appName('mapreduce2').getOrCreate()

data_sample = ["Data Scientist Training",
              "Data Engineer Training",
              "Data Analyst Training",
              "Data Architect Training",
              "AI Engineer Training"]

rdd = spark.sparkContext.parallelize(data_sample)

# Mapping step: split each sentence into words
rdd_words= rdd.flatMap(lambda linha: linha.split())

# Mapping step: map each word to a tuple (word, 1)
rdd_count = rdd_words.map(lambda word: (word, 1))

# Reduction step: sum all counts for each word
rdd_result = rdd_count.reduceByKey(lambda a, b: a + b)

fresult = rdd_result.collect()

for (palavra, contagem) in fresult:
    print(f'{palavra}: {contagem}')



    