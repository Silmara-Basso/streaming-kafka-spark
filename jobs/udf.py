# UDF (User Defined Function)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nUDF:\n')

spark = SparkSession.builder.appName('UDF').getOrCreate()

data_sample = [("1", "Wayne Gretzky"),
               ("2", "Alex Ovechkin"),
               ("3", "Mario Lemieux")]

cols = ["Seqno", "Name"]
df = spark.createDataFrame(data = data_sample, schema = cols)
df.show(truncate=False)

# Defining a function to convert each part of the name
def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
       resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

# Registering the function as a UDF (User Defined Function)
convertUDF = udf(lambda z: convertCase(z))

# Applying the UDF to the DataFrame and displaying the results
df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)

# Defining a UDF with a decorator to convert the name to uppercase
@udf(returnType=StringType()) 
def upperCase(str):
    return str.upper()

# Creating a lambda UDF for use with the upperCase function
upperCaseUDF = udf(lambda z: upperCase(z), StringType())

# Applying the UDF to create a column with uppercase names and displaying the results
df.withColumn("Name", upperCase(col("Name"))).show(truncate=False)

# Registering the UDF for use in SQL queries
spark.udf.register("convertUDF", convertCase, StringType())

# Creating a temporary view of the DataFrame
df.createOrReplaceTempView("LABTABLE1")

# Executing a SQL query that uses the registered UDF and displaying the results
spark.sql("select Seqno, convertUDF(Name) as Name from LABTABLE1").show(truncate=False)

# Running a SQL query to filter names that include 'Wayne' using the UDF
spark.sql("select Seqno, convertUDF(Name) as Name from LABTABLE1 " + \
          "where Name is not null and convertUDF(Name) like '%Wayne%'") \
     .show(truncate=False)

# Updating the list of tuples to include an element with a null name
data_sample = [("1", "Wayne Gretzky"),
               ("2", "Alex Ovechkin"),
               ("3", "Mario Lemieux"),
               ('4', None)]

# Recreating the DataFrame with the new data
df2 = spark.createDataFrame(data = data_sample, schema = cols)
df2.show(truncate=False)
df2.createOrReplaceTempView("LABTABLE2")
    
# Registering a UDF that handles null values ​​safely
spark.udf.register("_nullsafeUDF", lambda str: convertCase(str) if str is not None else "", StringType())

# Executing a SQL query that uses the null-safe UDF
spark.sql("select _nullsafeUDF(Name) from LABTABLE2").show(truncate=False)


# Running a SQL query to filter names that include 'Wayne' using the null-safe UDF
spark.sql("select Seqno, _nullsafeUDF(Name) as Name from LABTABLE2 " + \
          "where Name is not null and _nullsafeUDF(Name) like '%Wayne%'") \
     .show(truncate=False)


# A decorator in Python is a function that modifies or extends the behavior of another function or method 
# without changing it directly. Decorators are a powerful and elegant way to implement cross-cutting aspects 
# (such as logging, authentication, caching, etc.) in a modular and reusable way.


