# Convert Column to Map

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, create_map

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nConvert Column to Map:\n')

spark = SparkSession.builder.appName('Column_to_map').getOrCreate()

data_sample = [("98712","Sales",3000,"Portugal"), 
             ("57014","Sales",5000,"Spain"), 
             ("32986","HR",3900,"Portugal"), 
             ("10958","Marketing",2500,"Chile"), 
             ("67023","HR",6500,"England") ]

schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('country', StringType(), True)
     ])

df = spark.createDataFrame(data = data_sample, schema = schema)
df.printSchema()
df.show(truncate=False)

# Modify the DataFrame to add a map column (propertiesMap) 
# that contains salary and country, then removes the original 'salary' and 'country' columns
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),  # Add salary to map
        lit("country"),col("country")         # Add country to map
        )).drop("salary","country")       # Removes the original 'salary' and 'country' columns

df.printSchema()
df.show(truncate=False)

