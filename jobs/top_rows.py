# Top N

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nTop N:\n')

spark = SparkSession.builder.appName('TopN').getOrCreate()

data_sample = [("Bob",24),("Ana",44),
             ("Greg",43),("Laura",43),
             ("John",27),("Caroline",47)]

columns = ["cli_name","cli_age",]

df = spark.createDataFrame(data = data_sample, schema = columns)
df.show()

# Prints the first two records of the DataFrame
print(df.take(2))
print()

# Prints the last two records of the DataFrame
print(df.tail(2))
print()

# Prints the first two records of the DataFrame, similar to take(2)
print(df.head(2))
print()

# Prints the first record of the DataFrame
print(df.first())
print()

# Collects and prints all records in the DataFrame as a list of rows
print(df.collect())
print()

# Using Pandas
# Converts the first three records of the DataFrame to a Pandas DataFrame and prints
pandasDF = df.limit(3).toPandas()
print(pandasDF)
print()


