# Broadcast variables and RDD 

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nBroadcast variables and RDD:\n')

spark = SparkSession.builder.appName('broadcast').getOrCreate()

# Defines a dictionary with states and their respective cities
cities = {"RJ":"Cabo Frio", "SP":"Indaiatuba", "MG":"Contagem"}

# Creates a broadcast variable that will be sent to all nodes in the cluster only once
broadcast_variable = spark.sparkContext.broadcast(cities)

# Defines a list of tuples, each representing a person with first name, last name, country and state abbreviation
data_sample = [("Marcelo","Andrade","BRA","SP"),
             ("Isabel","Figueiredo","BRA","RJ"),
             ("Renato","Carvalho","BRA","MG"),
             ("Bianca","Serra","BRA","MG")]

rdd = spark.sparkContext.parallelize(data_sample)

# Define a function that uses the broadcast variable to convert the state abbreviation to the city name
def convert_data(code):
    return broadcast_variable.value[code]

# Transforms the original RDD by mapping each element to include the city name instead of the state abbreviation
resultf = rdd.map(lambda x: (x[0],x[1],x[2],convert_data(x[3]))).collect()
print(resultf)

# Creates two DataFrames
df_large = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value"])
df_small = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "label"])

# Performs a join forcing the use of broadcast for the small DataFrame
df_joined = df_large.join(broadcast(df_small), "id")

# Show the result of the join
df_joined.show()



# Broadcast variables are a mechanism provided by Apache Spark to efficiently share immutable data across all nodes in the cluster. 
# Instead of sending this data along with each task, Spark sends a copy of the data to each worker node only once, 
# making it available as a local variable on each machine. This significantly reduces communication costs 
# and the amount of data transferred during distributed processing.

# Broadcast variables are especially useful when a large amount of data needs to be accessed by multiple tasks distributed across 
# various nodes, such as in join operations, lookups, or other computations that require access to shared data (like dimension 
# tables in data warehouse processing).

# You can use broadcast variables with DataFrames in Apache Spark, although the mechanism is a bit different from directly using 
# broadcast variables with RDDs. Instead of using the broadcast() method explicitly as you would with RDDs, Spark already optimizes 
# join operations on DataFrames automatically, especially in situations where a small table is repeatedly used in joins with larger 
# tables. This process is known as a broadcast join.

# When you perform a join operation between two DataFrames and one is significantly smaller than the other, Spark can automatically 
# decide to use a broadcast join. This means that it will automatically send the smaller DataFrame to all processing nodes, 
# allowing the join to be performed locally on each node without the need for expensive network shuffles, which are typical in 
# normal joins of large distributed DataFrames.


