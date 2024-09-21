# Window Functions

import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import percent_rank
from pyspark.sql.functions import ntile
from pyspark.sql.functions import cume_dist    
from pyspark.sql.functions import lag    
from pyspark.sql.functions import lead    
from pyspark.sql.functions import col, avg, sum, min, max, row_number 

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nWindow Functions:\n')

spark = SparkSession.builder.appName('window').getOrCreate()

data_sample = (("Tyna", "Sales", 3000),
             ("Carl", "Sales", 4600),
             ("Peter", "Sales", 4100),
             ("Mark", "HR", 3000),
             ("Danilo", "Sales", 3000),
             ("John", "HR", 3300),
             ("Ted", "HR", 3900),
             ("Ana", "Marketing", 3000),
             ("Andrew", "Marketing", 2000),
             ("Sebastiab", "Sales", 4100))

cols = ["name", "department", "salary"]

df = spark.createDataFrame(data = data_sample, schema = cols)
df.printSchema()
df.show(truncate=False)

# Defining a partitioning for window operations
windowSpec = Window.partitionBy("department").orderBy("salary")

# Adding a 'row_number' column that shows the row number within the specified partition
df.withColumn("row_number", row_number().over(windowSpec)).show(truncate=False)

#Adding a 'rank' column that shows the salary rank within the specified partition
df.withColumn("rank", rank().over(windowSpec)).show()

# Adding a 'dense_rank' column that shows the dense rank of the salary within the specified partition
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# Adding a 'percent_rank' column that shows the percentage of salary rank within the specified partition
df.withColumn("percent_rank", percent_rank().over(windowSpec)).show()

# Adding a 'ntile' column that splits data into 2 groups within the specified partition
df.withColumn("ntile", ntile(2).over(windowSpec)).show()

# Adding a 'cume_dist' column that shows the cumulative distribution of salary within the specified partition
df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()

# Adding a 'lag' column that returns the salary with an offset of 2 positions behind in the specified partition
df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()

# Adding a 'lead' column that returns the salary with a 2-position shift ahead in the specified partition
df.withColumn("lead", lead("salary", 2).over(windowSpec)).show()

# Defining a window specification for unordered aggregation
windowSpecAgg = Window.partitionBy("department")

# Adding multiple aggregation columns and displaying only the first row for each department
df.withColumn("row", row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row") == 1).select("department", "avg", "sum", "min", "max") \
  .show()


