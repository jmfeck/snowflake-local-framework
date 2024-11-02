#!/bin/bash

# Run snowflake_ingest_excel.py with the specified configuration files
python3 snowflake_ingest_excel.py "config/snowflake_conn.yaml" "config/sample_config_ingest.yaml"

# Run snowflake_query_trigger.py with the specified connection and query files
python3 snowflake_query_trigger.py "config/snowflake_conn.yaml" "queries/sample_query.sql"

# Run snowflake_table_extractor.py with the specified configuration files
python3 snowflake_table_extractor.py "config/snowflake_conn.yaml" "config/sample_config_extractor.yaml"

# Wait for user input before closing (optional for interactive use)
read -p "Press any key to continue..."