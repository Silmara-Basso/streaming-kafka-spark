# Aggregate e Stats

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, min, max, countDistinct, count, sum_distinct
from pyspark.sql.functions import first, last, kurtosis, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop
from pyspark.sql.functions import variance, var_samp, var_pop

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\n Aggregate e Stats Funcions:\n')

spark = SparkSession.builder.appName('Aggregate and Stats').getOrCreate()


data_sample = [("Ana", "Sales", 3000),
             ("Carl", "Sales", 4600),
             ("Mary", "Sales", 4100),
             ("Gabriel", "Accounting", 3000),
             ("Eduard", "Sales", 3000),
             ("Philip", "Accounting", 3300),
             ("Silmara", "Accounting", 3900),
             ("Noah", "Marketing", 3000),
             ("Leo", "Marketing", 2000),
             ("Wesley", "Sales", 4100)]

schema = ["emp_name", "department", "salary"]
df = spark.createDataFrame(data = data_sample, schema = schema)
df.printSchema()
df.show(truncate=False)

# Calculates and prints distinct salary count
print("Distinct salary: " + str(df.select(approx_count_distinct("salary")).collect()[0][0]))
print()

# Calculates and prints average salaries
print("Average salary: " + str(df.select(avg("salary")).collect()[0][0]))
print()

df.select(collect_list("salary")).show(truncate=False)

# Without repetitions
df.select(collect_set("salary")).show(truncate=False)

df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)

print("Distinct Department and Salary Count: "+str(df2.collect()[0][0]))
print()

# Calculates and prints total salary count
print("Count: "+str(df.select(count("salary")).collect()[0]))
print()

df.select(first("salary")).show(truncate=False)
df.select(last("salary")).show(truncate=False)

# Calculates and displays the kurtosis of salaries
df.select(kurtosis("salary")).show(truncate=False)

df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)

# Calculates and displays salary asymmetry
df.select(skewness("salary")).show(truncate=False)

# Calculates and displays the sample and population standard deviation of salaries
df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)

df.select(sum("salary")).show(truncate=False)

# Calculates and displays the sum of different salaries
df.select(sum_distinct("salary")).show(truncate=False)

# Calculates and displays the sample and population variance of salaries
df.select(variance("salary"),var_samp("salary"),var_pop("salary")).show(truncate=False)
