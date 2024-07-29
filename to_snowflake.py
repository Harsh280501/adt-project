import boto3
import configparser
import snowflake.connector

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

# Snowflake Configuration
sf_account = get_config_value("SNOWFLAKE", "SNOWFLAKE_ACCOUNT")
sf_user = get_config_value("SNOWFLAKE", "SNOWFLAKE_USER")
sf_password = get_config_value("SNOWFLAKE", "SNOWFLAKE_PASSWORD")
sf_warehouse = get_config_value("SNOWFLAKE", "SNOWFLAKE_WAREHOUSE")
sf_database = get_config_value("SNOWFLAKE", "SNOWFLAKE_DATABASE")
sf_schema = get_config_value("SNOWFLAKE", "SNOWFLAKE_SCHEMA")

# Initialize AWS resources
s3 = boto3.resource('s3', region_name="us-east-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name="us-east-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

# List objects in S3 bucket
bucket = s3.Bucket("harsh-adt-bucket")
log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='sql/data')]
print(log_data_files)

# Get IAM role ARN
roleARN = iam.get_role(RoleName=get_config_value("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']
print(f"IAM Role ARN: {roleARN}")

# Initialize Snowflake connection
sf_conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# Create database and schema in Snowflake
def create_database_and_schema(database_name, schema_name):
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name};")
        print(f"Database {database_name} and schema {schema_name} created or already exists.")
    finally:
        cursor.close()

# Create table and load data from S3
def create_table_and_load_data():
    cursor = sf_conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {sf_database};")
        cursor.execute(f"USE SCHEMA {sf_schema};")
        
        # Create a staging table to load raw JSON data
        cursor.execute("""
        CREATE OR REPLACE TABLE employees_staging (
            raw VARIANT
        );
        """)

        # Create a stage to reference the S3 bucket
        cursor.execute(f"""
        CREATE OR REPLACE STAGE my_stage 
        URL='s3://harsh-adt-bucket/sql/' 
        CREDENTIALS=(AWS_KEY_ID='{KEY}' AWS_SECRET_KEY='{SECRET}');
        """)
        
        # Load data into the staging table
        cursor.execute("""
        COPY INTO employees_staging
        FROM @my_stage
        FILE_FORMAT = (type = 'JSON');
        """)
        
        # Create the final table
        cursor.execute("""
        CREATE OR REPLACE TABLE employees (
            id INT AUTOINCREMENT PRIMARY KEY,
            name STRING,
            position STRING,
            salary NUMBER(10, 2),
            hire_date DATE,
            department STRING,
            email STRING,
            phone_number STRING,
            address STRING
        );
        """)

        # Insert data into the final table from the staging table
        cursor.execute("""
        INSERT INTO employees (name, position, salary, hire_date, department, email, phone_number, address)
        SELECT 
            raw:name::STRING,
            raw:position::STRING,
            raw:salary::NUMBER,
            raw:hire_date::DATE,
            raw:department::STRING,
            raw:email::STRING,
            raw:phone_number::STRING,
            raw:address::STRING
        FROM employees_staging;
        """)

        # Select and print data from the final table
        cursor.execute("SELECT * FROM employees;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    finally:
        cursor.close()

# Create database and schema
create_database_and_schema(sf_database, sf_schema)

# Create table and load data from S3
create_table_and_load_data()

# Close Snowflake connection
sf_conn.close()
