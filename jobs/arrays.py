# Working with Array Type e StructType

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import array
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import col, concat_ws

#To clear the terminal windown
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nWorking with Array Type e StructType:\n')

spark = SparkSession.builder.appName('Arrays').getOrCreate()

# Data Sample DataFrame
data_sample = [("John,Smith",["Python","Rust","C++"],["Scala","Ruby"],"NY","AK"),
             ("Liam,Williams",["Java","Python","C++"],["PHP","Perl"],"CA","TX"),
             ("Noah,Brown",["PHP","Java"],["Ruby","Python"],"NY","CA"),
             ("Emma,Jones",["Scala","Java"],["Ruby","Python"],"MT","AK"),
             ("Oliver,Miller",["PHP","Java"],["Ruby","Python"],"TX","AK"),
             ("Charlotte,Davis",["C++","Java"],["Ruby","Python"],"AK","NY")]

# Defines an Array column type with Strings, without accepting null values 
arrayCol = ArrayType(StringType(),False)

schema = StructType([ 
    StructField("candidates_name", StringType(),True), 
    StructField("most_used_languages", ArrayType(StringType()),True), 
    StructField("less_used_languages", ArrayType(StringType()),True), 
    StructField("previous_state", StringType(), True), 
    StructField("current_state", StringType(), True) 
  ])

df = spark.createDataFrame(data = data_sample, schema = schema)
df.printSchema()

df.show()

# Using the explode function - each value in the array becomes a row in the dataframe
df.select(df.candidates_name, explode(df.most_used_languages)).show()

# Selects and displays the name column divided into an array
df.select(split(df.candidates_name,",").alias("namesAsArray")).show()

# Select and displays the list of states current and previous
df.select(df.candidates_name, array(df.previous_state,df.current_state).alias("estateAsArray")).show()

# filtering for "Python"
df.select(df.candidates_name, array_contains(df.most_used_languages,"Python").alias("uses_python")).show()

# Create a new DataFrame, converting the 'most_used_languages' column from an array to a string, separated by commas
df2 = df.withColumn("most_used_languages", concat_ws(",",col("most_used_languages")))

df2.printSchema()
df2.show(truncate=False)