# Operations with Dates

# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nOperations with Dates:\n')

# Session
spark = SparkSession.builder.appName('Add_Month').getOrCreate()

# Data Sample
sample_data = [("2024-01-23",1), ("2024-06-14",2), ("2024-09-29",3)]

# With Spark structions
# The expr function is used to evaluate the SQL expression and execute it.
# Spark instructions is more recommended, especially when the volume of data is large. SQL mode is slower

spark.createDataFrame(sample_data).toDF("date","increment") \
          .select(col("date"),col("increment"), \
            expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))") \
            .alias("inc_data")) \
          .show()

df = spark.createDataFrame(sample_data).toDF("date","increment")

# With SQL instructions
df.createOrReplaceTempView("DFTEMP")

spark.sql("""
    SELECT
        date,
        increment,
        ADD_MONTHS(TO_DATE(date, 'yyyy-MM-dd'), CAST(increment AS INT)) AS inc_date
    FROM
        DFTEMP
""").show()




