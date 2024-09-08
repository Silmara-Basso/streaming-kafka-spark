# Convert Map to Column

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, MapType

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nConverting Map to Column:\n')

spark = SparkSession.builder.appName('Map_to_Column').getOrCreate()

schema = StructType([
    StructField("cli_name", StringType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

data_sample = [("Silmara", {"age": "44", "city": "Toronto"}),
               ("Cleber", {"age": "38", "city": "Quebec"}),
               ("Philip", {"age": "15", "city": "Montreal"})]

df = spark.createDataFrame(data = data_sample, schema = schema)
df.show(truncate=False)

# Convert the map to columns
# Using getItem to access the values ​​of specific map keys
df = df.withColumn("age", col("attributes").getItem("age")) \
       .withColumn("city", col("attributes").getItem("city")) \
       .drop("attributes")

# Mostra o DataFrame após a conversão do map para colunas
df.show(truncate=False)
