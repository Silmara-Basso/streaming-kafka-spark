# query distinct data and perform deduplication

import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDedup amd Distinct data:\n')

spark = SparkSession.builder.appName('dedup').getOrCreate()

# Data Sample to test
data_sample = [("Robert", "Sales", 30000), \
             ("Michael", "Sales", 46000), \
             ("July", "Sales", 41000), \
             ("Mary", "Accounting", 30000), \
             ("Robert", "Sales", 30000), \
             ("Mark", "Accounting", 33000), \
             ("Jenifer", "Accounting", 39000), \
             ("Ana", "Marketing", 30000), \
             ("Ana", "Marketing", 30000), \
             ("Silmara", "Sales", 41000)]

data_cols = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = data_sample, schema = data_cols)

df.printSchema()

print("Table Count: " + str(df.count()) + "\n")

df.show(truncate=False)

# Distinct list
distinctDF = df.distinct()

print("Distinct Count: " + str(distinctDF.count()) + "\n")

# without truncating cell values
distinctDF.show(truncate=False)

# Dedup - removing duplicates based on all columns
df2 = df.dropDuplicates()

print("Deduplicate records: " + str(df2.count()) + "\n")

df2.show(truncate=False)

# Removing duplicates based on 'department' and 'salary' columns only
dropDisDF = df.dropDuplicates(["department", "salary"])

print("Distinct Count (department and salary): " + str(dropDisDF.count()) + "\n")

dropDisDF.show(truncate=False)
