# Collect

import os
import pyspark
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nCollect:\n')

spark = SparkSession.builder.appName('collect').getOrCreate()

data_sample = [("Data Science", 10), ("Technology", 20), ("Marketing", 30), ("Sales", 40)]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=data_sample, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# Collects all data from the DataFrame into a list of Row objects, moving the data to local memory
dataCollect = deptDF.collect()
print(dataCollect)

# Selects and collects data from the 'dept_name' column of the DataFrame, resulting in a list of Row objects
dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

# Iterates over each Row in the collected list and prints the department name and ID, formatted as a string
for row in dataCollect:
    print(row['dept_name'] + "," + str(row['dept_id']))


