# Withcolumn

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nWithcolumn:\n')

spark = SparkSession.builder.appName('withcolumn').getOrCreate()

data_sample = [('Michel','','Smith','1991-04-01','M',3000),
             ('John','Willians','','2000-05-19','M',4000),
             ('Robert','Carl','Smith','1978-09-05','M',4000),
             ('Susan','Chris','Starley','1967-12-01','F',4000),
             ('Margaret','Telles','Morgan','1980-02-17','F',-1)]

colunas = ["firstname", "homename", "surname", "birth", "gender", "salary"]

df = spark.createDataFrame(data = data_sample, schema = colunas)
df.printSchema()
df.show(truncate=False)

# Converts the 'salary' column to the Integer type
df2 = df.withColumn("salary",col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

# Multiply the value of the 'salary' column by 100
df3 = df.withColumn("salary",col("salary")*100)
df3.printSchema()
df3.show(truncate=False)

# Create a new column 'CopiedColumn' by copying and inverting the sign of the 'salario' column
df4 = df.withColumn("CopiedColumn",col("salary")* -1)
df4.printSchema()

# Adds a constant column 'Country' with the value "USA"
df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

# Adds two constant columns, 'Country' with "USA" and 'anotherColumn' with "anotherValue"
df6 = df.withColumn("Country", lit("USA")) \
   .withColumn("anotherColumn",lit("anotherValue"))
df6.printSchema()

# Renames the 'gender' column to 'sex' and shows the DataFrame with the new column name
df.withColumnRenamed("gender","gen").show(truncate=False)

# Deletes the column 'CopiedColumn' from df4 and shows the resulting DataFrame
df4.drop("CopiedColumn").show(truncate=False)

dataStruct = [(("James","","Smith"),"36636","M","3000"), \
              (("Michael","Rose",""),"40288","M","4000"), \
              (("Robert","","Williams"),"42114","M","4000"), \
              (("Mary","Anne","Jones"),"39192","F","4000"), \
              (("Jen","Mary","Brown"),"","F","-1")]

# Defines a complex schema with nested types for the DataFrame
schemaStruct = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('homename', StringType(), True),
             StructField('surname', StringType(), True)
             ])),
          StructField('birth', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])

df7 = spark.createDataFrame(data = dataStruct, schema = schemaStruct)
df7.printSchema()
df7.show(truncate=False)


