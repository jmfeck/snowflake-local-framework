# -*- coding: utf-8 -*-

# ==============================================================================
#   Author: João Manoel Feck
#   Email: joaomfeck@gmail.com
#   GitHub: https://github.com/jmfeck
# ==============================================================================

import yaml
import sys
import snowflake.connector
import os

###############################################################################################################
# Config file reader
###############################################################################################################
def load_config(file_path):
    """Load configuration from a YAML file."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    print(f"Configuration loaded from {file_path}")
    return config

# Load YAML connection configuration file from command-line arguments
config_conn_path = sys.argv[1]
config_conn = load_config(config_conn_path)

###############################################################################################################
# Query file reader
###############################################################################################################
sql_file_path = sys.argv[2]
with open(sql_file_path, 'r') as sql_file:
    sql_queries = sql_file.read()
sql_queries_list = [query.strip() for query in sql_queries.split(';') if query.strip()]

###############################################################################################################
# Functions
###############################################################################################################
def create_snowflake_session(username, auth_type, acc, wh, role=None, database=None, schema=None):
    try:
        SnowflakeSession = snowflake.connector.connect(
            user=username,
            authenticator=auth_type,
            account=acc,
            warehouse=wh,
            database=database,
            schema=schema,
            role=role
        )
        print("Snowflake connected")
        return SnowflakeSession
    except Exception as e:    
       print(f"Snowflake did not connect due to {e}")

###############################################################################################################
# Get config values
###############################################################################################################

# Snowflake connection parameters from config_conn
acc = config_conn['snow_conn_param']['acc']
auth_type = config_conn['snow_conn_param']['auth_type']
role = config_conn['snow_conn_param'].get('role', None)
username = config_conn['snow_conn_param']['username']
wh = config_conn['snow_conn_param']['wh']

########################## Main ##########################

# Establish Snowflake connection
conn = create_snowflake_session(username, auth_type, acc, wh, role)

# Execute each SQL query
for sql_query in sql_queries_list:
    print(f"Executing query: {sql_query}")
    conn.cursor().execute(sql_query)
