# This is the script to create the customer table in SQLite  
# Import
# pip install sqlite3
import sqlite3

data_path = "/Users/silmarabasso/repos/streaming-kafka-spark/dados/"
# Conecta ao banco de dados SQLite (cria se o arquivo não existir)
connection_db = sqlite3.connect(data_path + 'customer.db')

# Cria a tabela
cursor = connection_db.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS tb_customer (
    customer_name TEXT,
        customer_age INTEGER,
        customer_salary FLOAT,
        customer_city TEXT
)
""")

# Grava e fecha a conexão
connection_db.commit()
connection_db.close()
