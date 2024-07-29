import pandas as pd
from kafka import KafkaProducer
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
    
    # Extract table data
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()
    
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]
    
    # Extract schema information
    schema_info = extract_schema_info(cursor)
    
    cursor.close()
    connection.close()
    
    return data, schema_info

def extract_schema_info(cursor):
    schema_info = {}

    # Get table columns and their data types
    cursor.execute("""
    SELECT 
        TABLE_NAME, 
        COLUMN_NAME, 
        DATA_TYPE 
    FROM 
        INFORMATION_SCHEMA.COLUMNS 
    WHERE 
        TABLE_NAME = 'employees'
    """)
    columns = cursor.fetchall()
    schema_info['Columns'] = [dict(zip([column[0] for column in cursor.description], col)) for col in columns]

    # Get primary keys
    cursor.execute("""
    SELECT 
        KU.TABLE_NAME AS TableName, 
        KU.COLUMN_NAME AS ColumnName 
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
        INNER JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
        ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
        TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
    """)
    primary_keys = cursor.fetchall()
    schema_info['PrimaryKeys'] = [dict(zip([column[0] for column in cursor.description], pk)) for pk in primary_keys]

    # Get foreign keys
    cursor.execute("""
    SELECT 
        f.name AS ForeignKey,
        OBJECT_NAME(f.parent_object_id) AS TableName,
        COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
        OBJECT_NAME(f.referenced_object_id) AS ReferencedTableName,
        COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferencedColumnName
    FROM 
        sys.foreign_keys AS f
    INNER JOIN 
        sys.foreign_key_columns AS fc 
        ON f.object_id = fc.constraint_object_id
    """)
    foreign_keys = cursor.fetchall()
    schema_info['ForeignKeys'] = [dict(zip([column[0] for column in cursor.description], fk)) for fk in foreign_keys]

    return schema_info

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(CustomEncoder, self).default(obj)

def produce_data_to_kafka(data, schema):
    producer = KafkaProducer(
        bootstrap_servers=['18.191.110.113:9092'],
        value_serializer=lambda x: json.dumps(x, cls=CustomEncoder).encode('utf-8'))
    
    # Produce data records
    for record in data:
        producer.send('demo_testing2', value={'type': 'data', 'value': record})
    
    # Produce schema information
    producer.send('demo_testing2', value={'type': 'schema', 'value': schema})
    
    producer.flush()

if __name__ == "__main__":
    # Extract data and schema info
    consumer_data, schema_info = extract_from_sql_server()
    
    # Produce data and schema to Kafka
    produce_data_to_kafka(consumer_data, schema_info)
    
    # Print the data and schema for verification
    df = pd.DataFrame(consumer_data)
    print(df.head())
    print(json.dumps(schema_info, indent=2))
