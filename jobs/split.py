# Split

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nSplit:\n')

spark = SparkSession.builder.appName('split').getOrCreate()

data_sample = [('Michel','','Smith','1981-04-01'),
             ('John','Willians','','2001-05-19'),
             ('Susan','','Starley','1979-09-05'),
             ('Margaret','Telles','Morgan','1997-12-01'),
             ('Robert','Carl','Smith','1990-02-17')]

cols = ["name", "middlename", "surname", "birth_date"]

df = spark.createDataFrame(data_sample, cols)
df.printSchema()
df.show(truncate=False)

# Add separate columns for year, month and day from the 'birth_date' column using the split() function
df1 = df.withColumn('year', split(df['birth_date'], '-').getItem(0)) \
        .withColumn('month', split(df['birth_date'], '-').getItem(1)) \
        .withColumn('day', split(df['birth_date'], '-').getItem(2))
df1.printSchema()
df1.show(truncate=False)

# Alternative to the split() function
split_col = pyspark.sql.functions.split(df['birth_date'], '-')
df2 = df.withColumn('year', split_col.getItem(0)) \
        .withColumn('month', split_col.getItem(1)) \
        .withColumn('day', split_col.getItem(2))
df2.show(truncate=False)

# Use the split() function directly when selecting columns to add year, month and day with aliases
split_col = pyspark.sql.functions.split(df['birth_date'], '-')

# with select
df3 = df.select("name", "middlename", "surname", "birth_date", split_col.getItem(0).alias('year'), split_col.getItem(1).alias('month'), split_col.getItem(2).alias('day'))
df3.show(truncate=False)

# Creates a DataFrame with a string that will be split using regular expressions
df4 = spark.createDataFrame([('cerRWezauueAvemantesdeBnoalfabeto',)], ['str',])

# Selects the column and splits it into substrings where 'A' or 'B' occurs, without limiting the number of results
df4.select(split(df4.str, '[AB]').alias('str')).show(truncate=False)

# Splits the string into substrings where 'A' or 'B' occurs, with a maximum limit of 2 substrings
df4.select(split(df4.str, '[AB]', 2).alias('str')).show(truncate=False)







