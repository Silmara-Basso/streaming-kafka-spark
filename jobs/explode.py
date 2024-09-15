# Explode de Array e Map

import os
import pyspark
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nExplode de Array e Map:\n')

spark = SparkSession.builder.appName('Explode').getOrCreate()

data_sample = [('Clauss', ['Java', 'Scala'], {'level': 'senior', 'courses': 'IBTA'}),
             ('Michel', ['Rust', 'Java', None], {'level': 'pleno', 'courses': None}),
             ('Robert', ['Python', ''], {'nivel': 'junior', 'courses': ''}),
             ('John', None, None),
             ('Jeff', ['Uva', 'Melancia'], {})]

df = spark.createDataFrame(data = data_sample, schema = ['name', 'langs', 'attributes'])
df.printSchema()
df.show(truncate=False)

# Import the 'explode' function to expand list elements into separate lines
from pyspark.sql.functions import explode

# Select the name and expand the 'languages' column into separate separate lines
df2 = df.select(df.name, explode(df.langs))
df2.printSchema()
df2.show(truncate=False)

# Import the 'explode' function again, this time to expand the dictionary elements on separate lines
from pyspark.sql.functions import explode

# Select the name and expand the 'attributes' column into separate lines
df3 = df.select(df.name, explode(df.attributes))
df3.printSchema()
df3.show()

# Import the 'explode_outer' function which works like 'explode', but includes lines with null values
from pyspark.sql.functions import explode_outer

# Expand the 'langs' column into separate lines, including names with null lists
df.select(df.name, explode_outer(df.langs)).show()

# Expands the 'attributes' column into separate rows, including names with null or empty dictionaries
df.select(df.name, explode_outer(df.attributes)).show()

# Imports the 'posexplode' function which expands lists into separate lines and includes the element index in the list
from pyspark.sql.functions import posexplode

# Expands the 'langs' column, including the index of each language
df.select(df.name, posexplode(df.langs)).show()

# Expands the 'attributes' column, including the key as an index
df.select(df.name, posexplode(df.attributes)).show()

# Imports the 'posexplode_outer' function which works like 'posexplode' but includes rows with null values
from pyspark.sql.functions import posexplode_outer

# Expande a coluna 'langs', incluindo o índice de cada linguagem, com tratamento para listas nulas
df.select(df.name, posexplode_outer(df.langs)).show()

# Expande a coluna 'atributos', incluindo a chave como índice, com tratamento para dicionários nulos ou vazios
df.select(df.name, posexplode_outer(df.attributes)).show()

# Nested Array Explodes (Explode de Array Aninhados)

data_sample2 = [
  ("Mary",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Bob",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Elly",[["Python","Rust"],["Spark","Python"]])]

df2 = spark.createDataFrame(data = data_sample2, schema = ['name','languages'])
df2.printSchema()
df2.show(truncate=False)

# Select the name and explode the 'languages' column to transform each internal list into a new line
df2.select(df2.name, explode(df2.languages)).show(truncate=False)

# Select the name and apply the flatten function in the 'languages' column to combine the internal lists into a single list
from pyspark.sql.functions import flatten
df2.select(df2.name, flatten(df2.languages)).show(truncate=False)
