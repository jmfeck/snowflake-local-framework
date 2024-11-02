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
from snowflake.connector.pandas_tools import write_pandas
import re

###############################################################################################################
# Config file reader
###############################################################################################################
def load_config(file_path):
    """Load configuration from a YAML file."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    print(f"Configuration loaded from {file_path}")
    return config

# Load the YAML configuration files from command-line arguments
config_conn_path = sys.argv[1]
config_conn = load_config(config_conn_path)

config_ingest_path = sys.argv[2]
config_ingest = load_config(config_ingest_path)

###############################################################################################################
# Functions
###############################################################################################################
    
def create_snowflake_session(username, auth_type, acc, warehouse, database, schema, role):
    try:
        SnowflakeSession = snowflake.connector.connect(
            user=username,
            authenticator=auth_type,
            account=acc,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        print("Snowflake connected")
        return SnowflakeSession
    except Exception as e:    
       print(f"Snowflake did not connect due to {e}")
       
def export_df_to_snowflake(df, conn, table):
    # Prepare SQL query to create or replace table based on the DataFrame structure
    cols = str(df.dtypes.to_dict())
    cols = re.sub("{", "", cols)
    cols = re.sub("}", "", cols)
    cols = re.sub(":", "")
    cols = re.sub("dtype\\('O'\\)", "string", cols)
    cols = re.sub("dtype\\('int64'\\)", "integer", cols)
    cols = re.sub("dtype\\('float64'\\)", "double", cols)
    # Add any new type conversion here if needed
    cols = re.sub("'", "", cols)
    query = "CREATE OR REPLACE TABLE " + table + ' (' + cols + ')'
    print(query)
    conn.cursor().execute(query)
    write_pandas(conn, df, table.upper())

###############################################################################################################
# Get config values
###############################################################################################################

# Snowflake connection parameters from config_conn
username = config_conn['snow_conn_param']['username']
auth_type = config_conn['snow_conn_param']['auth_type']
acc = config_conn['snow_conn_param']['acc']
wh = config_conn['snow_conn_param']['wh']
role = config_conn['snow_conn_param']['role']

# Database and table information from config_ingest
database = config_ingest['snow_param']['database']
schema = config_ingest['snow_param']['schema']
tablename = config_ingest['snow_param']['tablename']

# Project parameters from config_ingest
input_foldername = config_ingest['project_param']['input_foldername']

# File parameters from config_ingest
file_radc = config_ingest['file_param']['file_radcname']
file_ext = config_ingest['file_param']['file_ext']
sheet_name = config_ingest['file_param']['sheet_name']

########################## Main ##########################
# Construct file path based on input folder and file details
path_script = os.path.dirname(os.path.abspath(__file__))
path_project = os.path.dirname(path_script)
path_input = os.path.join(path_project, input_foldername)
path_file = os.path.join(input_foldername, file_radc + file_ext)

# Create Snowflake connection and read Excel file
conn = create_snowflake_session(username, auth_type, acc, wh, database, schema, role)
df = pd.read_excel(path_file, sheet_name=sheet_name)

# Export DataFrame to Snowflake
export_df_to_snowflake(df, conn, tablename)
