# Working with Rows

import os
from pyspark.sql import SparkSession, Row

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nWorking with Rows:\n')

spark = SparkSession.builder.appName('Row').getOrCreate()

row = Row("Silmara", 1.75)

# Prints Row values, accessing them by index
print(row[0] + "," + str(row[1]))
print() 

# Add row
row2 = Row(cli_name = "Philip", cli_height = 1.90)

print(row2.cli_name)
print()  

# Create instances of Person
Person = Row("cli_name", "cli_height")
p1 = Person("Silmara", 1.75)
p2 = Person("Philip", 1.90)
print(p1.cli_name + "," + p2.cli_name)
print() 

# Define sample data with Rows, including names, languages, and states
data_sample = [Row(cli_name="Ana Rotef",lang=["Python","PHP","C++"],state="CA"), 
             Row(cli_name="Carl Andrens",lang=["Rust","Python","C++"],state="NJ"),
             Row(cli_name="Philip Willians",lang=["Julia","Ruby"],state="NJ"),
             Row(cli_name="Silmara Basso",lang=["Rust","Python"],state="NV")]

rdd = spark.sparkContext.parallelize(data_sample)
collData = rdd.collect()
print(collData)
print() 

for row in collData:
    # Prints the name and languages ​​(lang) of each Row
    print(row.cli_name + "," + str(row.lang))

print()  

#   Defines a new Row structure called Person with named fields
Person = Row("cli_name","lang","state")
data_sample = [Person("Ana Rotef",["Python","Ruby","C++"],"CA"), 
             Person("Carl Andrens",["Spark","Python","C++"],"NJ"),
             Person("Silmara Basso",["Rust","PHP"],"NV")]

rdd = spark.sparkContext.parallelize(data_sample)
collData = rdd.collect()
print(collData)
print()  

for person in collData:
    print(person.cli_name + "," +str(person.lang))
print()

# Creates a DataFrame from Person data
df = spark.createDataFrame(data_sample)

df.printSchema()
print()  
df.show()
print()  

collData = df.collect()
print(collData)
print()  

for row in collData:
    print(row.cli_name + "," +str(row.lang))
print()  
    
# Creates a new list of data, this time as tuples
data_sample = [("Ana Rotef",["Python","Ruby","C++"],"CA"), 
             ("Carl Andrens",["Rust","Python","C++"],"NJ"),
             ("Silmara Basso",["CSharp","PHP"],"NV")]

columns = ["cli_name","lang","state"]
df = spark.createDataFrame(data_sample).toDF(*columns)
df.printSchema()
print()  

for row in df.collect():
    print(row.cli_name)
print() 
