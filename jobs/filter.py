# Filters

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nUsing filters in Spark:\n')

spark = SparkSession.builder.appName('filters').getOrCreate()

# data sample
data_sample = [
        (("John","","Willians"),["Java","Scala","C++"],"CA","M"),
        (("Susan","Smith",""),["Spark","Java","C++"],"AL","F"),
        (("Margaret","","Jones"),["Rust","Go"],"AZ","F"),
        (("David","Brend","Smith"),["Rust","Go"],None,None),
        (("Elizabeth","Telles","Morgan"),["Rust","Go"],"FL","M"),
        (("Robert","Carl","Smith"),["Python","Go"],"CA","M")]
        
# Define o schema de Struct Aninhada
schema_data_sample = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('surname', StringType(), True)
             ])),
         StructField('langs', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True)
         ])

df = spark.createDataFrame(data = data_sample, schema = schema_data_sample)
df.printSchema()
print("Full Dataframe:")
df.show(truncate=False)

# Equal relationship
print("Equality Filters:")
df.filter(df.state == "CA").show(truncate=False)
df.filter(col("state") == "CA").show(truncate=False)  
df.filter("gender == 'M'").show(truncate=False)  

# Inequality relationship
print("Inequality Filters:")
df.filter(~(df.state == "CA")).show(truncate=False)
df.filter(df.state != "CA").show(truncate=False)  
df.filter("gender <> 'M'").show(truncate=False)     

# Create a list of states
state_list= ["CA","AL","FL"]

print("More Filters:")

# Filters the DataFrame rows where the value of the 'state' column is in the 'state_list' list
df.filter(df.state.isin(state_list)).show(truncate=False)

# not in
df.filter(~df.state.isin(state_list)).show(truncate=False)

# Filters the DataFrame rows where the state is "SP" and the gender is "M", displaying the results without truncating the values
df.filter((df.state == "CA") & (df.gender == "M")).show(truncate=False)

# Attempting to filter rows based on start of name but results in error due to incorrect access to 'name' field
#df.filter(df.name.startswith("M")).show()

# Filters lines where the first name starts with "M"
df.filter(df.name.firstname.startswith("M")).show(truncate=False)

# Filters lines where the first name ends with "n"
df.filter(df.name.firstname.endswith("n")).show(truncate=False)

# Filters the lines where the last name is equal to "Smith", displaying the results without truncating the values
df.filter(df.name.surname == "Smith").show(truncate=False)

# Cria dados de exemplo
data_sample2 = [(1,"James Smith"), 
              (2,"Michael Rose"), 
              (3,"Susan Morgan"), 
              (4,"Robert Smith"), 
              (5,"Margaret Smith")]

# Cria o dataframe com a definição do schema
df2 = spark.createDataFrame(data = data_sample2, schema = ["id","name"])

print("Others filters:")

# Filters lines where the 'name' field starts with "M"
df2.filter(df2.name.like("M%")).show(truncate=False)

# Filters lines where the 'name' field contains the substring "Smith" anywhere in the text
df2.filter(df2.name.like("%mith%")).show(truncate=False)

# Filter lines where the 'name' field matches the regular expression that identifies names 
# that end with "Smith", ignoring case differences.
df2.filter(df2.name.rlike("(?i).*smith$")).show(truncate=False)

print("Null filters:")

# Filters rows where the 'state' column value is null using SQL syntax     
df.filter("state is NULL").show()

# Filter the rows where the value of the 'state' column is null using the DataFrame function
df.filter(df.state.isNull()).show()

# Filters rows where the value of the 'state' column is null using 'col' function to reference the column
df.filter(col("state").isNull()).show()

# Filters rows where both 'state' and 'gender' are null using SQL syntax
df.filter("state IS NULL AND gender IS NULL").show()

# Filters rows where both 'state' and 'gender' are null using DataFrame functions
df.filter(df.state.isNull() & df.gender.isNull()).show()

# Filters rows where the 'state' column value is not null using different approaches
df.filter("state is not NULL").show()
df.filter("NOT state is NULL").show()
df.filter(df.state.isNotNull()).show()
df.filter(col("state").isNotNull()).show()

# Remove as linhas onde o valor da coluna 'estado' é nulo
df.na.drop(subset=["state"]).show()

# Creates or replaces a temporary view with name 'DATA' for the current DataFrame
df.createOrReplaceTempView("DATA")

# Executes SQL queries on the 'DATE' view, filtering rows based on the presence of null values ​​in 'STATUS' and/or 'GENDER'
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()