import csv
from pyhive import hive
import pandas as pd
from hdfs import InsecureClient
import os
import pymysql
import numpy as np

# Function to connect to Hive


def connect_to_hive():
    try:
        hive_conn = hive.Connection(
            host='hive-server', port=10000, username='hive')
        print("Hive connection established successfully!")
        return hive_conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

# Function to upload file to HDFS


def upload_to_hdfs(local_path, hdfs_path):
    try:
        hdfs_client = InsecureClient(
            'http://hadoop-namenode:50070', user='hive')
        print("HDFS connection established successfully!")

        if not os.path.exists(local_path):
            print(f"Local file {local_path} does not exist.")
            return False

        hdfs_client.makedirs('/user/hive/warehouse', permission=777)
        print(f"Created HDFS directory: {hdfs_path}")

        hdfs_client.upload(hdfs_path, local_path, overwrite=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
        return True
    except Exception as e:
        print(f"Error uploading to HDFS: {e}")
        return False

# Function to fetch MySQL table schema


def get_mysql_table_schema(table_name):
    try:
        mysql_conn = pymysql.connect(
            host='mysql', user='hive', password='hive', database='sakila')
        query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sakila' AND TABLE_NAME = '{table_name}'"
        df_schema = pd.read_sql(query, mysql_conn)
        mysql_conn.close()
        return df_schema
    except Exception as e:
        print(f"Error fetching MySQL schema for table {table_name}: {e}")
        return None

# Function to create or replace Hive table


def create_or_replace_hive_table(hive_conn, table_name, df_schema):
    try:
        cursor = hive_conn.cursor()

        # Drop table if it exists
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        print(f"Dropped table {table_name} if it existed.")

        # Define column mappings and create table query
        type_mapping = {
            'tinyint': 'smallint',
            'smallint': 'smallint',
            'mediumint': 'int',
            'int': 'int',
            'bigint': 'bigint',
            'float': 'float',
            'double': 'double',
            'decimal': 'decimal(10,2)',
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
            data_type_hive = type_mapping.get(data_type_mysql, 'string')
            hive_columns.append(f"`{column_name}` {data_type_hive}")

        create_table_query = f"""
        CREATE TABLE {table_name} ({', '.join(hive_columns)})
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
        """
        cursor.execute(create_table_query)
        print(f"Table {table_name} created successfully in Hive.")
    except Exception as e:
        print(f"Error creating or replacing table {table_name} in Hive: {e}")
# Function to load data into Hive from HDFS


def load_data_into_hive(hive_conn, table_name, hdfs_path):
    try:
        cursor = hive_conn.cursor()
        load_data_query = f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name}"
        cursor.execute(load_data_query)
        print(f"Data loaded into Hive table {table_name} from {hdfs_path}")
    except Exception as e:
        print(f"Error loading data into Hive table {table_name}: {e}")

# Function to upload MySQL data to Hive


def upload_mysql_to_hive(table_name):
    try:
        mysql_conn = pymysql.connect(
            host='mysql', user='hive', password='hive', database='sakila')
        query = f"SELECT * FROM sakila.{table_name}"
        df = pd.read_sql(query, mysql_conn)
        mysql_conn.close()

        # Process and format data
        if df is not None and not df.empty:
            # Replace NaN values with empty strings for string columns and 0 for numeric columns
            for column in df.columns:
                if df[column].dtype == 'object':
                    df[column] = df[column].fillna('')
                else:
                    df[column] = df[column].fillna(0)

            local_path = f'./tables/{table_name}.csv'
            hdfs_path = f'/user/hive/warehouse/{table_name}.csv'

            # Save DataFrame to CSV without header, using tab as delimiter
            df.to_csv(local_path, index=False, header=False,
                      na_rep='', sep='\t', quoting=csv.QUOTE_MINIMAL)

            hive_conn = connect_to_hive()
            if hive_conn:
                create_or_replace_hive_table(
                    hive_conn, table_name, get_mysql_table_schema(table_name))
                if upload_to_hdfs(local_path, hdfs_path):
                    load_data_into_hive(hive_conn, table_name, hdfs_path)
                hive_conn.close()

            print(f"Uploaded {table_name} to HDFS and loaded into Hive.")
        else:
            print(f"No data found in MySQL table {table_name}")
    except Exception as e:
        print(
            f"Error uploading MySQL data to Hive for table {table_name}: {e}")


# Testing script
try:
    mysql_conn = pymysql.connect(
        host='mysql', user='hive', password='hive', database='sakila')
    all_tables = pd.read_sql("SHOW TABLES", mysql_conn)
    mysql_conn.close()

    for table_name in all_tables['Tables_in_sakila']:
        print(f"Uploading {table_name} to HDFS and loading into Hive...")
        upload_mysql_to_hive(table_name)

    print('='*80)
    hive_conn = connect_to_hive()
    if hive_conn:
        try:
            cursor = hive_conn.cursor()
            # Fetch tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            print("Tables:")
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                rows_count = cursor.fetchone()[0]  # Fetch the count value
                print(f"Table: {table_name}, Rows: {rows_count}")

        except Exception as e:
            print(f"Error fetching tables or row counts: {e}")

        finally:
            hive_conn.close()  # Close connection after use
    else:
        print("Hive connection failed.")

except Exception as e:
    print(f"Error: {e}")
