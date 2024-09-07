# More about Expr

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


os.environ['TERM'] = 'xterm'
os.system('clear')
print('\nSome examples of Expression Expr :\n')

spark = SparkSession.builder.appName('expressions').getOrCreate()

# Create Sample data
data_sample = [("Laura","Martins"), ("Rodrigo","Silva")] 
df = spark.createDataFrame(data_sample).toDF("first_name","last_name") 

# Cria nova coluna em tempo de execução
df.withColumn("full_name", expr("first_name || ' ' || last_name")).show()

data_sample = [("Laura","F"), ("Silmara","F"), ("Philipe","M"), ("Mary","")]
columns = ["name", "gender"]

df = spark.createDataFrame(data = data_sample, schema = columns)
df.show()

# Add new column
df2 = df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))

df2.show()

# + data
data_sample = [("2025-01-15",1), ("2025-06-22",2), ("2025-09-27",3)] 

# Cria o dataframe com nomes para as colunas
df = spark.createDataFrame(data_sample).toDF("date","increment") 

# Adiciona meses com o incremento e cria o alias
df.select(df.date, df.increment,
     expr("add_months(date,increment)")
  .alias("inc_data1")).show()

# Adiciona meses com o incremento com alias dentro da expressão
df.select(df.date, df.increment,
     expr("""add_months(date,increment) as inc_data2""")
  ).show()

# Adiciona meses com o incremento modificado com alias dentro da expressão
df.select(df.date, df.increment + 5,
     expr("""add_months(date,increment + 5) as inc_data3""")
  ).show()



