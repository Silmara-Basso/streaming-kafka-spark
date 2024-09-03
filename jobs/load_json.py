# This is the script to load json data to SQLite with Spark

import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, regexp_replace

# Spark connection
spark = SparkSession.builder \
     .appName("json_project") \
     .getOrCreate()

# Data Schema
schema = types.StructType([
    types.StructField("customer_name", types.StringType(), True),
    types.StructField("customer_age", types.IntegerType(), True),
    types.StructField("customer_email", types.StringType(), True),
    types.StructField("customer_salary", types.FloatType(), True),
    types.StructField("customer_city", types.StringType(), True)
])

# Load file - the file must be on the docker volumes

df_json = spark.read.schema(schema).json("data/customer.json")

# Drop email column
df_json_drop_email = df_json.drop("customer_email")

df = df_json_drop_email.filter(
    (col("customer_age") > 35) &
    (col("customer_city") == "Chicago") &
    (col("customer_salary") < 45000)
)

# Schema Validation
df.printSchema()
df.show()

# Schema is empty
if df.rdd.isEmpty():
    print("File empty")
else:
    df_clean = df.withColumn("customer_name", regexp_replace(col("customer_name"), "@", ""))

    # SQLite 
    sqlite_db_path = os.path.abspath("data/customer.db")
    sqlite_uri = "jdbc:sqlite://" + sqlite_db_path
    properties = {"driver": "org.sqlite.JDBC"}

    # Verify SQLite table
    try:
        spark.read.jdbc(url=sqlite_uri, table='tb_customer', properties=properties)
        write_mode = "append"
    except:
        write_mode = "overwrite"

    df_clean.write.jdbc(url=sqlite_uri, table="tb_customer", mode=write_mode, properties=properties)

    print(f"Data inserted in 'customer.db' with '{write_mode}' method")


