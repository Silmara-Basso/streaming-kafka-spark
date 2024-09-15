# Loop

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nSpark Loop:\n')

spark = SparkSession.builder.appName('loop').getOrCreate()

data_sample = [('Alessander','Smith','M',30),
             ('Susan','Willians','F',41),
             ('David','Starley','M',62)]


cols = ["firstname", "surname", "gender", "salary"]

df = spark.createDataFrame(data = data_sample, schema = cols)

print('Original Dataframe:')
df.show()

# Selects and displays the concatenated full name, gender and double salary
print('Dataframe with Concat:')
df.select(concat_ws(",", df.firstname, df.surname).alias("name"), df.gender, lit(df.salary * 2).alias("new_salary")).show()

# Collects data from the DataFrame and prints to the console
print('Dataframe with Collect:')
print(df.collect())

# Converts the DataFrame to an RDD and applies a transformation to concatenate first and last name, double the salary
rdd = df.rdd.map(lambda x: (x[0] + "," + x[1], x[2], x[3] * 2))  

# Converts the RDD back to a DataFrame with named columns
df2 = rdd.toDF(["name", "gender", "new_salary"])

# Displays the new DataFrame
print("--------")
print('Dataframe After Manipulating Data with RDD:')
df2.show()

# Use a lambda function to print data from the RDD
df.rdd.foreach(lambda x: print("Data ==>"+x["firstname"]+","+x["surname"]+","+x["gender"]+","+str(x["salary"] * 2)))

# Collects data from the DataFrame and stores it in a variable
dataCollect = df.collect()

# Iterates over the collected data and prints first and last name
print("--------")
print('Printing Dataframe Columns via ROW:')
for row in dataCollect:
    print(row['firstname'] + "," + row['surname'])

# Converts the RDD to a local iterator and iterates over it, printing first and last name
dataCollect = df.rdd.toLocalIterator()

# Converts Spark DataFrame to Pandas DataFrame
import pandas as pd
pandasDF = df.toPandas()

# Iterates over the Pandas DataFrame and prints first name and gender
print("--------")
print('Print name and gender:')
for index, row in pandasDF.iterrows():
    print(row['firstname'], row['gender'])
