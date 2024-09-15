# Drop de Colunas

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDrop de Columns:\n')

spark = SparkSession.builder.appName('Drop_Columns').getOrCreate()

data_sample = (("Carl", None, "Smith", "16636", "Washington", 13100), \
             ("Susan", "Rose", "", "20288", "Boston", 14300), \
             ("Robert", "", "Williams", "12114", "New Orleans", 11400), \
             ("Mary", "Anne", "Jones", "19192", "Washington", 15500), \
             ("David", "Mary", "Brown", "24561", "Boston", 13000) \
            )

columns = ["firstname", "middlename", "surname", "id", "city", "salary"]
df = spark.createDataFrame(data = data_sample, schema = columns)
df.printSchema()
df.show(truncate=False)

# Remove 'firstname' column from DataFrame and display new schema
df.drop("firstname").printSchema()
  
# Remove the 'firstname' column from the DataFrame using the 'col' function and display the new schema
df.drop(col("firstname")).printSchema()  
  
# Removes the 'firstname' column directly by accessing it as an attribute of the DataFrame and displays the new schema
df.drop(df.firstname).printSchema()

# Removes the 'firstname', 'middlename' and 'lastname' columns from the DataFrame and displays the new schema
df.drop("firstname", "middlename", "surname").printSchema()

# Defines a tuple with the names of the columns to be removed
cols = ("firstname", "middlename", "surname", "city")

# Removes the columns specified in the 'cols' tuple from the DataFrame and displays the new schema
df.drop(*cols).printSchema()

# Removes all rows that contain at least one null value and displays the results without truncating
df.na.drop().show(truncate=False)

# Remove all lines that contain at least one null value (equivalent to previous example) 
df.na.drop(how="any").show(truncate=False)

# Remove rows that contain null values ​​in the columns specified in 'subset' ('state' and 'mmiddlename')
df.na.drop(subset=["city", "middlename"]).show(truncate=False)

# Remove all rows that contain at least one null value (alternative method using 'dropna')
df.dropna().show(truncate=False)



