# Operators

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nOperators:\n')

spark = SparkSession.builder.appName('operators').getOrCreate()

data_sample = [("Eric", "Clapton", "100", None),
             ("Eddie", "Van Halen", "200", 'M'),
             ("Carl", "Sant", "None", ''),
             ("Jimi Hendrix", None, "400", 'M'),
             ("Mark", "Knopfler", 500, 'M')] 

cols = ["firstname", "surname", "id", "gender"]

df = spark.createDataFrame(data_sample, cols)

# Alias
print('\nOperator alias():')
df.select(df.firstname.alias("first_name"), \
          df.surname.alias("last_name"), \
          expr("firstname ||','|| surname").alias("fullname")).show()

# asc, desc
print('\nOperators asc() e desc():')
df.sort(df.firstname.asc()).show()
df.sort(df.firstname.desc()).show()

# cast
print('\nCast for int value:')
df.select(df.firstname, df.id.cast("int")).printSchema()

# between
print('\nOperador between():')
df.filter(df.id.between(100,300)).show()

# contains
print('\nOperador contains():')
df.filter(df.firstname.contains("Mark")).show()

# startswith e endswith
print('\nOperadores startswith() e endswith():')
df.filter(df.firstname.startswith("E")).show()
df.filter(df.firstname.endswith("k")).show()

# isNull e isNotNull
print('\nOperadores isNull() e isNotNull():')
df.filter(df.surname.isNull()).show()
df.filter(df.surname.isNotNull()).show()

# substr
print('\nOperador substr():')
df.select(df.firstname.substr(1,2).alias("substr")).show()

