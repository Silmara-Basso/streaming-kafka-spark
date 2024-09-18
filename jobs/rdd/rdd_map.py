# RDD Map

# Imports
import os
from pyspark.sql import SparkSession

os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nRDD Map:\n')

spark = SparkSession.builder.appName('rdd_map').getOrCreate()
    

data_sample = [('Carlos','Estrela','M',30), ('Tatiana','Moraes','F',41), ('Renato','Carvalho','M',62)]

cols = ["first_name", "last_name", "gender", "salary"]

df = spark.createDataFrame(data = data_sample, schema = cols)
df.show()

# Mapping each row of the DataFrame to a new shape, combining first and last name, keeping gender and doubling salary
my_rdd = df.rdd.map(lambda x: (x[0] + " " + x[1], x[2], x[3] * 2))  

# Convertendo o RDD mapeado de volta para um DataFrame e mostrando o resultado
df2 = my_rdd.toDF(["name", "gender", "new_salary"]).show()

# Mapping each row of the DataFrame using columns by name, adjusting the format and doubling the salary
my_rdd = df.rdd.map(lambda x: (x["first_name"] + " " + x["last_name"], x["gender"], x["salary"] * 2)) 

# Converting the mapped RDD back to a DataFrame and showing the result
df3 = my_rdd.toDF(["name", "gender", "new_salary"]).show()

# Mapping each row of the DataFrame using attributes directly, adjusting the format and doubling the salary
my_rdd = df.rdd.map(lambda x: (x.first_name + " " + x.last_name, x.gender, x.salary * 2)) 

# Converting the mapped RDD back to a DataFrame and showing the result
df4 = my_rdd.toDF(["name", "gender", "new_salary"]).show()

# Defining a function that performs similar operations to the previous mapping, but in a separate function

def my_func(x):
    first_name = x.first_name
    last_name = x.last_name
    fname = first_name + " " + last_name
    gender = x.gender.lower()
    salary = x.salary * 2
    return (fname, gender, salary)


# Applying the function to the RDD and converting the result to a DataFrame, then displaying it
df5 = df.rdd.map(lambda x: my_func(x)).toDF().show()

# Another way to apply the function to RDD and convert to DataFrame, displaying the result
df6 = df.rdd.map(my_func).toDF().show()



