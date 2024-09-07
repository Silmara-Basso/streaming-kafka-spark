# More about Expr

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr 


os.environ['TERM'] = 'xterm'
os.system('clear')
print('\nSome examples of Expression Expr :\n')

spark = SparkSession.builder.appName('expressions').getOrCreate()

# Create Sample data
data_sample = [("Laura","Martins"), ("Rodrigo","Silva")] 
df = spark.createDataFrame(data_sample).toDF("first_name","last_name") 

# Add new colums with expr concat ||
df.withColumn("full_name", expr("first_name || ' ' || last_name")).show()

data_sample = [("Laura","F"), ("Silmara","F"), ("Philipe","M"), ("Mary","")]
columns = ["name", "gender"]

df = spark.createDataFrame(data = data_sample, schema = columns)
df.show()

# Add new column with expr case sql
df2 = df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))

df2.show()

# + date
data_sample = [("2025-01-15",1), ("2025-06-22",2), ("2025-09-27",3)] 

df = spark.createDataFrame(data_sample).toDF("date","increment") 

# Alias - example 1
df.select(df.date, df.increment,
     expr("add_months(date,increment)")
  .alias("inc_data1")).show()

# Alias - example 2
df.select(df.date, df.increment,
     expr("""add_months(date,increment) as inc_data2""")
  ).show()

# Increment date + 5 month
df.select(df.date, df.increment + 5,
     expr("""add_months(date,increment + 5) as inc_data3""")
  ).show()



