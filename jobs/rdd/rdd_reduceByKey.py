# ReduceByKey

# Imports
import os
from pyspark.sql import SparkSession

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nReduceByKey:\n')

spark = SparkSession.builder.appName('ReduceByKey').getOrCreate()

data_sample = [('Scientist', 1),
              ('Architect', 1),
              ('Architect', 1),
              ('Analyst', 1),
              ('Manager', 1),
              ('Engineer', 1),
              ('Scientist', 1),
              ('Engineer', 1),
              ('Analyst', 1),
              ('Scientist', 1),
              ('Engineer', 1),
              ('Scientist', 1),
              ('Engineer', 1)]

rdd = spark.sparkContext.parallelize(data_sample)

# Applying the reduceByKey operation to add the counters for each profession
rdd2 = rdd.reduceByKey(lambda a,b: a+b)

# Collecting and printing RDD results after reduction
for element in rdd2.collect():
    print(element)
