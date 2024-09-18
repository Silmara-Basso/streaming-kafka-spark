# RDD Actions

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD Actions:\n')

spark = SparkSession.builder.appName('rdd_actions').getOrCreate()

data_sample = [("A", 10),("B", 20),("C", 30),("D", 40),("E", 30),("F", 60)]

# Create an RDD from the list of tuples
inputRDD = spark.sparkContext.parallelize(data_sample)

# Create another RDD from a list of numbers, with duplicates
listRdd = spark.sparkContext.parallelize([8,3,6,7,5,3,2,2,4,6,2,4,7,4,1])

#Count the number of elements in the RDD
print("Count: " + str(listRdd.count()))
# Output: Count: 15

# Estimates the number of elements in the RDD using a timeout of 1200 milliseconds
print("countApprox: " + str(listRdd.countApprox(1200)))
# Output: countApprox: 15

# Estimate the number of distinct elements in listRdd
print("countApproxDistinct: " + str(listRdd.countApproxDistinct()))
# Output: countApproxDistinct: 8

# Estimate the number of distinct elements in the inputRDD
print("countApproxDistinct: " + str(inputRDD.countApproxDistinct()))
# Output: countApproxDistinct: 6

# Counts the number of times each value occurs in the RDD
print("countByValue:  " + str(listRdd.countByValue()))

# Returns the first element of the RDD
print("First element:  " + str(listRdd.first()))

# Returns the first tuple of the inputRDD
print("First tuple:  " + str(inputRDD.first()))

# Returns the two largest elements of listRdd
print("Two Major Elements: " + str(listRdd.top(2)))

# Returns the two largest tuples from the inputRDD 
print("Two Largest Tuples: " + str(inputRDD.top(2)))

# Returns the smallest element of listRdd
print("Lowest Value: " + str(listRdd.min()))

# Returns the smallest tuple from inputRDD
print("Lowest Value:  " + str(inputRDD.min()))

# Retorna o maior elemento do listRdd
print("Highest Value:  " + str(listRdd.max()))

# Returns the largest tuple from the inputRDD
print("Highest Value:  " + str(inputRDD.max()))

# Returns the first two elements of listRdd
print("First Two Elements: " + str(listRdd.take(2)))

# Returns the first two elements of listRdd when sorted
print("First Two Elements (Ordered): " + str(listRdd.takeOrdered(2)))


# Actions are operations that instruct Spark to perform calculations and return the final result of these calculations. 
# They are used to obtain concrete results from transformed or untransformed RDDs. 
# Unlike transformations, which are "lazy" operations and only assemble an execution plan, 
# actions force the execution of these plans and bring the data to the driver program or write it to disk.



