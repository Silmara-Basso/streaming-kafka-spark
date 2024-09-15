# Maptype

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nMaptype:\n')

spark = SparkSession.builder.appName('Maptype').getOrCreate()

data_sample = [('Eric',{'level':'senior','specialty':'Data Engineering'}),
             ('Susan',{'level':'pleno','specialty':None}),
             ('Jimi',{'level':'junior','specialty':'Machine Learning'}),
             ('Mark',{'level':'junior','specialty':'Data Science'}),
             ('Sophy',{'level':'pleno','specialty':''})]

schema = StructType([
    StructField('name', StringType(), True),
    StructField('attributes', MapType(StringType(),StringType()),True)
])
df = spark.createDataFrame(data = data_sample, schema = schema)
df.printSchema()
df.show(truncate=False)

# Transforms the DataFrame into an RDD for data manipulation and converts it back to a DataFrame with a specific structure
df3 = df.rdd.map(lambda x: (x.name,x.attributes["level"],x.attributes["specialty"])).toDF(["name", "level", "specialty"])

df3.printSchema()
df3.show(truncate=False)

# Adds new 'level' and 'specialty' columns to the original DataFrame extracted from the 'attributes' map, then removes the 'attributes' column
df.withColumn("level", df.attributes.getItem("level")) \
  .withColumn("specialty", df.attributes.getItem("specialty")) \
  .drop("attributes") \
  .show(truncate=False)

# Repeat the previous process, but using an alternative syntax to access the values ​​in the map
df.withColumn("level", df.attributes["level"]) \
  .withColumn("specialty", df.attributes["specialty"]) \
  .drop("attributes") \
  .show(truncate=False)

# Selects the name and explodes the attribute map into two separate columns: key and value
from pyspark.sql.functions import explode
df.select(df.name, explode(df.attributes)).show(truncate=False)

# Select name and extract keys from attribute map
from pyspark.sql.functions import map_keys
df.select(df.name, map_keys(df.attributes)).show(truncate=False)

# Select name and extract values ​​from attribute map
from pyspark.sql.functions import map_values
df.select(df.name, map_values(df.attributes)).show(truncate=False)

