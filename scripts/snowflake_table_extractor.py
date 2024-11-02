# -*- coding: utf-8 -*-

# ==============================================================================
#   Author: João Manoel Feck
#   Email: joaomfeck@gmail.com
#   GitHub: https://github.com/jmfeck
# ==============================================================================

import yaml
import sys
import snowflake.connector
import pandas as pd
import os
import re
import json

###############################################################################################################
# Config file reader
###############################################################################################################
def load_config(file_path):
    """Load configuration from a YAML file."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    print(f"Configuration loaded from {file_path}")
    return config

# Load YAML configuration files from command-line arguments
config_conn_path = sys.argv[1]
config_conn = load_config(config_conn_path)

config_extract_path = sys.argv[2]
config_extract = load_config(config_extract_path)

###############################################################################################################
# Functions
###############################################################################################################
def create_snowflake_session(username, auth_type, acc):
    try:
        SnowflakeSession = snowflake.connector.connect(
            user=username,
            authenticator=auth_type,
            account=acc
        )
        print("Snowflake connected")
        return SnowflakeSession
    except Exception as e:    
       print(f"Snowflake did not connect due to {e}")

def read_snowflake(conn, db, schema, table):
    cur = conn.cursor() 
    sql_dtypes = f"SHOW COLUMNS IN TABLE {db}.{schema}.{table}"
    cur.execute(sql_dtypes)
    cur.execute('SELECT * FROM TABLE(result_scan(last_query_id()))')
    df_dtypes = cur.fetch_pandas_all()
    df_dtypes['data_type'] = df_dtypes['data_type'].apply(lambda x: json.loads(x)['type'])
    dict_dtypes = dict(zip(df_dtypes['column_name'], df_dtypes['data_type']))

    sql = f"SELECT "
    list_conv_cols = [f'to_varchar({col}) AS {col}' if dict_dtypes[col] == 'BINARY' else f'"{col}"' for col in dict_dtypes]
    sql += ", ".join(list_conv_cols) + f' FROM {db}.{schema}.{table}'
    print(sql)
    cur.execute(sql)
    df_out = cur.fetch_pandas_all()
    cur.close()
    return df_out

###############################################################################################################
# Get config values
###############################################################################################################

# Snowflake connection parameters from config_conn
user = config_conn['snow_conn_param']['username']
authenticator = config_conn['snow_conn_param']['auth_type']
account = config_conn['snow_conn_param']['acc']

# Database and table information from config_extract
database = config_extract['snow_param']['database']
schema = config_extract['snow_param']['schema']
table = config_extract['snow_param']['tablename']

# Project parameters from config_extract
output_foldername = config_extract['project_param']['output_foldername']

# File parameters from config_extract
file_radc = config_extract['file_param']['file_radcname']
file_ext = config_extract['file_param']['file_ext']
file_sheetname = config_extract['file_param'].get('sheet_name', None)

########################## Main ##########################
path_script = os.path.dirname(os.path.abspath(__file__))
path_project = os.path.dirname(path_script)
path_output = os.path.join(path_project, output_foldername)
path_file = os.path.join(path_output, file_radc + file_ext)

# Establish Snowflake connection
conn = create_snowflake_session(user, authenticator, account)

# Read data from Snowflake
print("Reading table from Snowflake")
df = read_snowflake(conn, database, schema, table)

# Export the DataFrame based on the specified file format
print("Exporting table to file")
if file_ext == ".parquet":
    df.to_parquet(path=path_file, index=False)
elif file_ext == ".csv":
    df.to_csv(path_or_buf=path_file, index=False)
elif file_ext == ".xlsx":
    df.to_excel(path_file, sheet_name=file_sheetname, index=False)
else:
    print(f"Unsupported file extension: {file_ext}")
