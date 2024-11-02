python snowflake_ingest_excel.py "config/snowflake_conn.yaml" "config/sample_config_ingest.yaml"
python snowflake_query_trigger.py "config/snowflake_conn.yaml" "queries/sample_query.sql"
python snowflake_table_extractor.py "config/snowflake_conn.yaml" "config/sample_config_extractor.yaml"
PAUSE