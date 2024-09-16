# Operations with RDD

import os
from pyspark.sql import SparkSession
from operator import add


os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nOperations with RDD:\n')

spark = SparkSession.builder.appName('rdd').getOrCreate()

listRdd = spark.sparkContext.parallelize([1,2,3,4,5])
print("RDD Count:" + str(listRdd.count()))

# Defines a sequential operation to aggregate RDD elements
seqOp = (lambda x, y: x + y)

# Defines a combinatorial operation to join the results of the partitions
combOp = (lambda x, y: x + y)

#  Performs RDD aggregation using defined operations
agg = listRdd.aggregate(0, seqOp, combOp)

# Print the aggregation result
print(agg) # output 15

# Defines a new sequential operation to calculate sum and count
seqOp2 = (lambda x, y: (x[0] + y, x[1] + 1))

# Components of the seqOp2 Function:
# lambda: Indicates that an anonymous function is being created.
# x, y: These are the function parameters. x is a tuple, where x[0] represents a sum accumulator and x[1] a counter. y is an individual element of the RDD.
# x[0] + y: This is the first element of the result of the lambda function. Here, y (an element of the RDD) is added to x[0], which is the accumulator that holds the sum of the elements processed so far.
# x[1] + 1: This is the second element of the lambda function result. It represents the operation of incrementing the counter x[1] by one. This is used to count the number of elements that have been aggregated.

# Define a new combinatorial operation to join sums and counts of partitions
combOp2 = (lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Components of the combOp2 Function:
# lambda: Indicates that an anonymous function is being defined.
# x, y: These are the function parameters. Both x and y are tuples, and each contains two elements. The first element (x[0] or y[0]) is a sum accumulator, while the second element (x[1] or y[1]) is a counter.
# x[0] + y[0]: This is the first element of the result of the lambda function. Here, the first element of each tuple (both represent partial sums of different partitions of the RDD) is summed, thus combining the results of the partial aggregations into a total sum.
# x[1] + y[1]: This is the second element of the result of the lambda function. Here, the second element of each tuple (both represent element counts from different partitions of the RDD) is summed, resulting in the total element count.

# Performs aggregation with sum and count using the defined operations
agg2 = listRdd.aggregate((0, 0), seqOp2, combOp2)
print(agg2) # output (15,5)

# Performs a tree aggregation, which is a more efficient way of aggregating with large RDDs
agg2 = listRdd.treeAggregate(0, seqOp, combOp)
print(agg2) # output 15

# Performs a fold operation on RDD that is similar to reduce, but starts with an initial value
foldRes = listRdd.fold(0, add)
print(foldRes) # output 15

# Performs a reduction operation on the RDD to sum all elements
redRes = listRdd.reduce(add)
print(redRes) # output 15

# Define an anonymous function using lambda expression for addition
add = lambda x, y: x + y

# Performs a tree reduction, which is more efficient in terms of network communication
redRes = listRdd.treeReduce(add)
print(redRes) # output 15

# Collect all RDD elements
dados = listRdd.collect()
print(dados)
