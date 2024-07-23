import redshift_connector

# Redshift connection parameters
REDSHIFT_HOST = 'adt-cluster-project.cozj5ggvyxpd.us-east-2.redshift.amazonaws.com'
REDSHIFT_DBNAME = 'dev'
REDSHIFT_USER = 'awsuser'
REDSHIFT_PASSWORD = 'Harsh007'
REDSHIFT_PORT = 5439

# S3 and IAM Role
S3_PATH = 's3://harsh-adt-bucket/sql/'
IAM_ROLE = 'arn:aws:iam::533267037651:role/redshift-full-access'

# Establish connection to Redshift
conn = redshift_connector.connect(
    host=REDSHIFT_HOST,
    database=REDSHIFT_DBNAME,
    port=REDSHIFT_PORT,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)

# Create a Cursor object
cursor = conn.cursor()

print("Redshift cluster connected successfully")
# SQL statement to create the employees table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS employees (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255),
    position NVARCHAR(255),
    salary DECIMAL(10, 2),
    hire_date DATE,
    department NVARCHAR(255),
    email NVARCHAR(255),
    phone_number NVARCHAR(20),
    address NVARCHAR(255)
);
"""

# Execute the create table query
cursor.execute(create_table_query)
conn.commit()

# SQL COPY command to load data from S3 into the Redshift table
copy_query = f"""
COPY employees
FROM '{S3_PATH}'
IAM_ROLE '{IAM_ROLE}'
FORMAT AS JSON 'auto';
"""

# Execute the COPY command
cursor.execute(copy_query)
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Table created and data loaded successfully into Redshift")
