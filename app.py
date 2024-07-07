from pyhive import hive
import pandas as pd
from hdfs import InsecureClient
import os
import pymysql

print('\n\n\n\n\n\n')

# Hive connection details
try:
    hive_conn = hive.Connection(
        host='localhost', port=10000, username='hive')
    hive_cursor = hive_conn.cursor()
    print("Hive connection established successfully!")
except Exception as e:
    print(f"Error connecting to Hive: {e}")


def upload_to_hdfs(local_path, hdfs_path):
    try:
        # HDFS connection details
        hdfs_client = InsecureClient(
            'http://localhost:50070', user='hive')
        print("HDFS connection established successfully!")

        # Check if local file exists
        if not os.path.exists(local_path):
            print(f"Local file {local_path} does not exist.")
            return

        # Check if HDFS client is able to connect
        try:
            hdfs_client.status('/')
        except Exception as e:
            print(f"Unable to connect to HDFS: {e}")
            return

        # Make HDFS directory if it doesn't exist
        hdfs_client.makedirs('/user/hive/warehouse')
        print(f"Created HDFS directory: {hdfs_path}")

        # Upload file to HDFS
        print(f"Uploading {local_path} to {hdfs_path}")
        hdfs_client.upload(hdfs_path, local_path, overwrite=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
    except Exception as e:
        print(f"Error uploading to HDFS: {e}")

# Function to get table schema from MySQL


def get_mysql_table_schema(table_name):
    try:
        mysql_conn = pymysql.connect(host='localhost',
                                     user='hive',
                                     password='hive',
                                     database='sakila')
        query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sakila' AND TABLE_NAME = '{table_name}'"
        df_schema = pd.read_sql(query, mysql_conn)
        mysql_conn.close()
        return df_schema
    except Exception as e:
        print(f"Error fetching MySQL schema: {e}")

# Function to create table in Hive


def create_hive_table(table_name, df_schema):
    try:
        hive_conn = hive.Connection(host='localhost', port=10000)
        cursor = hive_conn.cursor()

        # Mapping MySQL data types to Hive data types
        type_mapping = {
            'tinyint': 'tinyint',
            'smallint': 'smallint',
            'mediumint': 'int',
            'int': 'int',
            'bigint': 'bigint',
            'float': 'float',
            'double': 'double',
            'decimal': 'decimal',
            'date': 'date',
            'datetime': 'timestamp',
            'timestamp': 'timestamp',
            'char': 'string',
            'varchar': 'string',
            'text': 'string',
            'longtext': 'string',
            'mediumtext': 'string',
            'tinytext': 'string',
            'binary': 'binary',
            'varbinary': 'binary',
            'blob': 'binary',
            'enum': 'string',
            'set': 'string',
            'code': 'string'
        }

        hive_columns = []
        for index, row in df_schema.iterrows():
            column_name = row['COLUMN_NAME']
            data_type_mysql = row['DATA_TYPE']
            # Adjust data type mapping as needed
            data_type_hive = type_mapping.get(data_type_mysql, 'string')
            # print(f"Column: {column_name}, Hive data type: {data_type_hive}")
            # Enclose column name in backticks if necessary
            hive_columns.append(f"`{column_name}` {data_type_hive}")
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(hive_columns)})"

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(create_table_query)
        print(f"Table {table_name} created successfully in Hive.")
    except Exception as e:
        print(f"Error creating table {table_name} in Hive: {e}")
    finally:
        hive_conn.close()

# Function to load data from HDFS into Hive


def load_data_into_hive(table_name, hdfs_path):
    try:
        hive_conn = hive.Connection(host='localhost', port=10000)
        cursor = hive_conn.cursor()

        load_data_query = f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name}"
        cursor.execute(load_data_query)
        print(f"Data loaded into Hive table {table_name} from {hdfs_path}")
    except Exception as e:
        print(f"Error loading data into Hive table {table_name}: {e}")
    finally:
        hive_conn.close()


# Function to upload data from MySQL to HDFS and load into Hive
def upload_mysql_to_hive(table_name):
    try:
        mysql_conn = pymysql.connect(host='localhost',
                                     user='hive',
                                     password='hive',
                                     database='sakila')
        query = f"SELECT * FROM sakila.{table_name}"
        df = pd.read_sql(query, mysql_conn)
        mysql_conn.close()
        local_path = f'./tables/{table_name}.csv'
        hdfs_path = f'/user/hive/warehouse/{table_name}.csv'

        # Create Hive table
        create_hive_table(table_name, get_mysql_table_schema(table_name))

        # Upload to HDFS
        df.to_csv(local_path, index=False)
        upload_to_hdfs(local_path, hdfs_path)

        # Load into Hive
        load_data_into_hive(table_name, hdfs_path)

        print(f"Uploaded {table_name} to HDFS and loaded into Hive.")
    except Exception as e:
        print(f"Error uploading MySQL data to Hive: {e}")


# Connect to MySQL and fetch all tables in sakila database
mysql_conn = pymysql.connect(host='localhost',
                             user='hive',
                             password='hive',
                             database='sakila')
all_tables = pd.read_sql("SHOW TABLES", mysql_conn)
mysql_conn.close()

# Iterate through each table, upload to HDFS, and load into Hive
for table_name in all_tables['Tables_in_sakila']:
    print(f"Uploading {table_name} to HDFS and loading into Hive...")
    upload_mysql_to_hive(table_name)

print('='*80)

# Iterate all tables in Hive and print table name and length of each table
hive_conn = hive.Connection(host='localhost', port=10000, username='hive')
cursor = hive_conn.cursor()
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Table: {table_name}, Count: {count}")

hive_conn.close()
