# Sampling

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nSampling:\n')

spark = SparkSession.builder.appName('Sampling').getOrCreate()

# Create a DataFrame with 100 numbers, from 0 to 99
df = spark.range(100)

print("Examples with sample()")
print()

# Prints a sample of approximately 6% of the data
print(df.sample(0.06).collect())
print()

# Prints a sample of approximately 10% of the data, with a specific seed for reproduction
print(df.sample(0.1,123).collect())
print()

# Repeat previous sampling to demonstrate consistency with the same seed
print(df.sample(0.1,123).collect())
print()

# Prints another 10% sample, but with a different seed, resulting in a different sample
print(df.sample(0.1,456).collect())
print()

print("Examples with replace")
print()

# Sample with replacement, approximately 30% of the data, with seed specified
print(df.sample(True,0.3,123).collect())
print()

# Sample without replacement, approximately 30% of the data, with seed specified
print(df.sample(0.3,123).collect())
print()

print("Examples with sampleBy")
print()
df2 = df.select((df.id % 3).alias("key"))
print(df2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect())
print()

print("Examples with RDD")
print()

rdd = spark.sparkContext.range(0,100)

print(rdd.sample(False,0.1,0).collect())
print()

print(rdd.sample(True,0.3,123).collect())
print()

print(rdd.takeSample(False,10,0))
print()

print(rdd.takeSample(True,30,123))
print()




