import boto3
import configparser
import snowflake.connector
import json
import re

# Load configuration
config = configparser.ConfigParser()
config.read('cluster.config')

# Function to get configuration values
def get_config_value(section, key):
    try:
        return config.get(section, key)
    except configparser.NoOptionError:
        raise ValueError(f"Missing '{key}' in section '{section}' in configuration file")

# AWS Configuration
KEY = get_config_value("AWS", "KEY")
SECRET = get_config_value("AWS", "SECRET")
S3_BUCKET = get_config_value("AWS", "S3_BUCKET")  # Define your S3 bucket name here

# Snowflake Configuration
sf_account = get_config_value("SNOWFLAKE", "SNOWFLAKE_ACCOUNT")
sf_user = get_config_value("SNOWFLAKE", "SNOWFLAKE_USER")
sf_password = get_config_value("SNOWFLAKE", "SNOWFLAKE_PASSWORD")
sf_warehouse = get_config_value("SNOWFLAKE", "SNOWFLAKE_WAREHOUSE")
sf_database = get_config_value("SNOWFLAKE", "SNOWFLAKE_DATABASE")
sf_schema = get_config_value("SNOWFLAKE", "SNOWFLAKE_SCHEMA")

# Initialize AWS resources
s3 = boto3.resource('s3', region_name="us-east-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
s3_client = boto3.client('s3', region_name="us-east-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name="us-east-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

# Function to fetch schema JSON from S3 and delete it after reading
def fetch_schema_from_s3(schema_key):
    try:
        obj = s3.Object(S3_BUCKET, schema_key)
        schema_json = obj.get()['Body'].read().decode('utf-8')
        
        # Delete the schema file after reading it
        delete_object_from_s3(schema_key)
        
        return json.loads(schema_json)
    except Exception as e:
        print(f"Error fetching schema from S3: {e}")
        raise

# Function to get schema file and table name
def get_schema_file_and_table_name():
    schema_files = [filename.key for filename in bucket.objects.filter(Prefix='sql/schema')]
    for schema_file in schema_files:
        match = re.match(r'sql/schema_(.+)\.json', schema_file)
        if match:
            table_name = match.group(1)
            return schema_file, table_name
    raise ValueError("No valid schema file found in the specified S3 bucket.")

# Create database and schema in Snowflake
def create_database_and_schema(database_name, schema_name):
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name};")
        print(f"Database {database_name} and schema {schema_name} created or already exists.")
    finally:
        cursor.close()

# Create table and load data from S3 based on schema
def create_table_and_load_data(schema, table_name):
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {sf_database};")
        cursor.execute(f"USE SCHEMA {sf_schema};")
        
        # Create a staging table to load raw JSON data
        cursor.execute(f"""
        CREATE OR REPLACE TABLE {table_name}_staging (
            raw VARIANT
        );
        """)

        # Create a stage to reference the S3 bucket
        cursor.execute(f"""
        CREATE OR REPLACE STAGE my_stage 
        URL='s3://{S3_BUCKET}/sql/' 
        CREDENTIALS=(AWS_KEY_ID='{KEY}' AWS_SECRET_KEY='{SECRET}');
        """)
        
        # Load data into the staging table
        cursor.execute(f"""
        COPY INTO {table_name}_staging
        FROM @my_stage
        FILE_FORMAT = (type = 'JSON');
        """)

        # Dynamically create the final table based on the schema
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ("
        for col in schema['Columns']:
            column_definition = f"{col['COLUMN_NAME']} {col['DATA_TYPE'].upper()}"
            if col['DATA_TYPE'].lower() == 'decimal':
                column_definition += "(10, 2)"
            create_table_sql += column_definition + ", "
        create_table_sql = create_table_sql.rstrip(', ') + ");"
        cursor.execute(create_table_sql)

        # Insert data into the final table from the staging table
        insert_sql = f"""
        INSERT INTO {table_name} (""" + ', '.join([col['COLUMN_NAME'] for col in schema['Columns']]) + """)
        SELECT 
        """
        insert_sql += ', '.join([f"raw:{col['COLUMN_NAME']}::{col['DATA_TYPE'].upper()}" for col in schema['Columns']])
        insert_sql += f" FROM {table_name}_staging"
        
        cursor.execute(insert_sql)

        # Add primary keys if any
        primary_keys = [pk['ColumnName'] for pk in schema.get('PrimaryKeys', [])]
        if primary_keys:
            alter_table_sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({', '.join(primary_keys)});"
            cursor.execute(alter_table_sql)

        # Add foreign keys if any
        for fk in schema.get('ForeignKeys', []):
            fk_name = fk.get('ForeignKey', f"fk_{table_name}_{fk['ColumnName']}")
            cursor.execute(f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk['ColumnName']})
            REFERENCES {fk['ReferencedTableName']}({fk['ReferencedColumnName']});
            """)

        # Select and print data from the final table
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    finally:
        cursor.close()

# Function to delete an object from S3
def delete_object_from_s3(object_key):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=object_key)
        print(f"Successfully deleted {object_key} from S3")
    except Exception as e:
        print(f"Error deleting object from S3: {e}")

if __name__ == "__main__":
    try:
        # List objects in S3 bucket
        bucket = s3.Bucket(S3_BUCKET)
        log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='sql/data')]
        print("Data files:", log_data_files)

        # Get schema file and table name
        schema_key, table_name = get_schema_file_and_table_name()
        print(f"Fetching schema from S3 with key: {schema_key}")

        # Fetch schema from S3
        schema = fetch_schema_from_s3(schema_key)

        # Initialize Snowflake connection
        sf_conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema
        )

        # Create database and schema
        create_database_and_schema(sf_database, sf_schema)

        # Create table and load data from S3
        create_table_and_load_data(schema, table_name)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close Snowflake connection
        if 'sf_conn' in locals() and sf_conn is not None:
            sf_conn.close()
