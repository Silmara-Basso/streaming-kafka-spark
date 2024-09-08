# Dictionary & Dataframe

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType, IntegerType
from pyspark.sql.functions import explode, map_keys, col


os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDictionary and Dataframe:\n')

spark = SparkSession.builder.appName('Dictionary').getOrCreate()

# data sample with dictionary
data_sample = [('Liam', {'category':'black','level':'brown'}),
             ('Sophy', {'category':'brown','level':None}),
             ('Noah', {'category':'red','level':'black'}),
             ('Emma', {'category':'grey','level':'grey'}),
             ('Carl', {'category':'brown','level':''})]


df = spark.createDataFrame(data = data_sample, schema = ['cli_name','cli_classification'])
df.printSchema()
df.show(truncate=False)


# Defines a schema with specific types, including MapType
schema = StructType([
    StructField('cli_name', StringType(), True),
    StructField('cli_classification', MapType(StringType(),StringType()),True)
])

# Creates a new DataFrame with the given data and defined schema
df2 = spark.createDataFrame(data = data_sample, schema = schema)
df2.printSchema()
df2.show(truncate=False)

# 4 ways to adds 'category' and 'level' columns to the original DataFrame, extracting values ​​from the classification map, 
# and remove the original 'classification' column

# using RDD
# Transforms the DataFrame to separate sorting information into distinct columns given data and defined schema 
print('\nFirst way:\n')
df3 = df.rdd.map(lambda x: \
      (x.cli_name, x.cli_classification["category"], x.cli_classification["level"])) \
      .toDF(["cli_name","category","level"])

df3.printSchema()
df3.show()


# getItem
print('\nSecond way:\n')
df.withColumn("category", df.cli_classification.getItem("category")) \
  .withColumn("level", df.cli_classification.getItem("level")) \
  .drop("classification") \
  .show()

# 
print('\nThird way:\n')
df.withColumn("category", df.cli_classification["category"]) \
  .withColumn("level", df.cli_classification["level"]) \
  .drop("classification") \
  .show()

# extract columns
print('\nFourth way:\n')
levelDF = df.select(explode(map_keys(df.cli_classification))).distinct()

# keys to list
levelList = levelDF.rdd.map(lambda x:x[0]).collect()

# Prepares a list of columns, dynamically creating columns based on map keys
levelCols = list(map(lambda x: col("cli_classification").getItem(x).alias(str(x)), levelList))
df.select(df.cli_name, *levelCols).show()



