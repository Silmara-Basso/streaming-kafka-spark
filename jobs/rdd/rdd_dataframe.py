# RDD to Dataframe

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD e Dataframe:\n')

spark = SparkSession.builder.appName('rdd_dataframe').getOrCreate()

dept = [("Sales",10), 
        ("Marketing",20), 
        ("HR",30), 
        ("Data Engineering",40)]

rdd = spark.sparkContext.parallelize(dept)
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name", "dept_id"]

# Creating a new DataFrame with the specified column names
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

# Creating a DataFrame directly with data and a specified column schema
deptDF = spark.createDataFrame(data = dept, schema = deptColumns)

deptDF.printSchema()
deptDF.show(truncate=False)


# Defining a schema for the DataFrame with data types and nullability options
deptSchema = StructType([       
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

# Creating a DataFrame with the given schema and data
deptDF1 = spark.createDataFrame(data = dept, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
