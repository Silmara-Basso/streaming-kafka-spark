# In this script I am testing log level (error only)

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nTesting log level:\n')

# Session
spark = SparkSession.builder.appName('Log-Level').getOrCreate()

# data create
data_input = [("Silmara", "Math", 2), ("John", "Science", 5), ("Julian", "Logic", 4)]
data_cols = ["teacher", "subject", "num_classes"]

df = spark.createDataFrame(data = data_input, schema = data_cols)

df.show(truncate=False)