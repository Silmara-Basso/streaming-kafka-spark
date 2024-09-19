# MapPartitions

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nMapPartitions:\n')

spark = SparkSession.builder.appName('MapPartitions').getOrCreate()

data_sample = [('Jeremy','Willians','M',3000),
             ('Mary','Rose','F',4100),
             ('Peter','Smith','M',6200)]

cols = ["firstname", "lastname", "gender", "salary"]
df = spark.createDataFrame(data = data_sample, schema = cols)
df.show()

# Repartition the DataFrame into 2 partitions
df = df.repartition(2)

# Define a function to reformat data, calculating 10% of salary as bonus
def reformat(partitionData):
    for row in partitionData:
       # Generates a list with full name and 10% of salary
        yield [row.firstname + " " + row.lastname, row.salary * 10/100]

# Applies the reformat function to the DataFrame RDD and converts the result back to a DataFrame
df.rdd.mapPartitions(reformat).toDF().show()

# Define another function to reformat data, storing intermediate results
def reformat2(partitionData):
    updatedData = []
    for row in partitionData:
        # Combines first and last name, calculates 10% of salary and adds to list
        name = row.firstname + " " + row.lastname
        bonus = row.salary * 10/100
        updatedData.append([name,bonus])
    # Returns an iterator over the updated data
    return iter(updatedData)

# Applies the second reformatting function to the original DataFrame RDD,
# specifying column names for the new DataFrame
df2 = df.rdd.mapPartitions(reformat2).toDF(["name","bonus"])
df2.show()



# In Apache Spark, mapPartitions is a transformation function that processes data in 
# RDDs (Resilient Distributed Datasets). It is an alternative to using the map function, but operates at a different scale. 
# While map applies a function to each element in the RDD individually, mapPartitions applies a function to 
# each partition of the RDD as a whole.


