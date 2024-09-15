# Column Operations
# Imports
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nColumn Operations:\n')

spark = SparkSession.builder.appName('Column_operations').getOrCreate()


data_sample = [("Noah",23), ("Sophy",40)]

df = spark.createDataFrame(data_sample).toDF("name.first_name", "gender")
df.printSchema()
df.show()

# Select and display only the 'name.first_name' column using different syntaxes
df.select(col("`name.first_name`")).show()
df.select(df["`name.first_name`"]).show()

# Substring
df.withColumn("new_col", col("`name.first_name`").substr(1,2)).show()

# Filters and shows lines where 'name.firstname' starts with "S"
df.filter(col("`name.first_name`").startswith("S")).show()

# Create a generator to replace dots with underscores in column names
new_cols = (column.replace('.', '_') for column in df.columns)
df2 = df.toDF(*new_cols)
df2.show()

# 2 ways to select
df.select(df.gender).show()
df.select(df["gender"]).show()

# Select and display the 'name.firstname' column using string indexing with the qualified name
df.select(df["`name.first_name`"]).show()

# Create a list of Row objects containing name and additional properties
data = [Row(name="Peter", prop=Row(level="senior", specialty="Data Science")),
        Row(name="Susan", prop=Row(level="pleno", specialty="Data Engineer"))]

df = spark.createDataFrame(data)
df.printSchema()

# Ways to select and display the 'level' subcolumn
df.select(df.prop.level).show()
df.select(df["prop.level"]).show()
df.select(col("prop.level")).show()

# Select and display all subcolumns within 'prop' using col("prop.*")
df.select(col("prop.*")).show()

# Creates a DataFrame with three numeric columns
data = [(100,2,1),(200,3,4),(300,4,4)]
df = spark.createDataFrame(data).toDF("col1","col2","col3")

# Numeric Operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
# rest of the division
df.select(df.col1 % df.col2).show()
# Performs comparisons between columns and shows the result
df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()


