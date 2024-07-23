import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json
import pyodbc
from decimal import Decimal
from datetime import date, datetime

def extract_from_sql_server():
    server = 'LAPTOP-OHGJHODB\\SQLEXPRESS'
    database = 'EmployeeDB'
    
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                'SERVER=' + server + ';'
                                'DATABASE=' + database + ';'
                                'Trusted_Connection=yes;')
    
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()
    
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    
    cursor.close()
    connection.close()
    
    return data

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(CustomEncoder, self).default(obj)

def produce_data_to_kafka(data):
    producer = KafkaProducer(
        bootstrap_servers=['18.191.110.113:9092'],
        value_serializer=lambda x: json.dumps(x, cls=CustomEncoder).encode('utf-8'))
    
    for record in data:
        producer.send('demo_testing2', value=record)
    
    producer.flush()

if __name__ == "__main__":

    consumer_data = extract_from_sql_server()
    produce_data_to_kafka(consumer_data)
    df = pd.DataFrame(consumer_data)
    print(df.head())
