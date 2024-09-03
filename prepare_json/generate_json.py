# generate a sample of 1000 records in JSON format

# Imports
import random
import json

# Example data generator
def generate_data(num_records):
    
    names = ["Mark", "Bob", "Carol", "John", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"]
    cities = ["New York City", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin"]

    for _ in range(num_records):
        customer_name = random.choice(names)
        customer_age = random.randint(20, 60)
        customer_email = f"{customer_name.lower()}@silcomp.com"
        customer_salary = random.randint(3000, 8000)
        customer_city = random.choice(cities)

        yield {"customer_name": customer_name, "customer_age": customer_age, "customer_email": customer_email, "customer_salary": customer_salary, "customer_city": customer_city}

# Cria 1000 registros

data_sample = list(generate_data(1000))

# Caminho do arquivo JSON
data_path = "/Users/silmarabasso/repos/streaming-kafka-spark/dados/"
file_path= data_path+'customer.json'

# Salvando os dados no arquivo JSON
with open(file_path, 'w') as file:
    for item in data_sample:
        json.dump(item, file)
        file.write('\n')