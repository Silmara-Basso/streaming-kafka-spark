# Add new columns in a dataframe

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import when
from pyspark.sql.functions import current_date


os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nAdd New Column:\n')


spark = SparkSession.builder.appName('DSAProjeto2-Script02').getOrCreate()

# Sample data
data_sample = [('Ana', 'Rotef', 'F', 3000),
             ('Carl', 'Andrens', 'M', 4100),
             ('Silmara', 'Basso', 'F', 6200)]

data_cols = ["employee_first_name", "employee_last_name", "gender", "salary"]


df = spark.createDataFrame(data = data_sample, schema = data_cols)

df.show()

# Checks if the 'percentage_bonus' column exists in the dataframe
if 'percentage_bonus' not in df.columns:

    print("The percentage_bonus column does not exist in the table. Adding...\n")
 
    df.withColumn("percentage_bonus", lit(0.3)).show()
    
df.withColumn("bonus_value", df.salary * 0.3).show()

df.withColumn("employee_name", concat_ws(",","employee_first_name",'employee_last_name')).show()

df.withColumn("current_date", current_date()).show()

df.withColumn("level", \
   when((df.salary < 4000), lit("A")) \
     .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
     .otherwise(lit("C")) \
  ).show()
    
# ADD Bonus
df.select("employee_first_name", "salary", lit(0.3).alias("bonus")).show()

# Bonus Value
df.select("employee_first_name", "salary", lit(df.salary * 0.3).alias("bonus_value")).show()

df.select("employee_first_name", "salary", current_date().alias("today_date")).show()

df.show()

df.createOrReplaceTempView("DFTEMP")

# SQL Query
spark.sql("select employee_first_name, salary, '0.3' as bonus from DFTEMP").show()

spark.sql("select employee_first_name, salary, salary * 0.3 as bonus_value from DFTEMP").show()

spark.sql("select employee_first_name, salary, current_date() as today_date from DFTEMP").show()

spark.sql("select employee_first_name, salary, " +
          "case when salary < 4000 then 'A' " +
          "else 'B' END as level from DFTEMP").show()






