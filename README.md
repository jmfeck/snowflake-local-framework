# Snowflake Local Framework

## Overview
The Snowflake Local Framework provides a set of tools to manage Snowflake operations directly from a local environment. This framework simplifies tasks such as uploading flat files, executing SQL queries, and exporting tables, all without the need for direct access to Snowflake. It’s ideal for development and testing, offering flexibility and control over data processes.

## Features
- **Upload Flat Files**: Load data from local files into Snowflake.
- **Execute SQL Queries**: Run SQL scripts stored locally.
- **Export Tables**: Save Snowflake tables to local files in formats like `.csv`, `.parquet`, or `.xlsx`.
- **Unified Interface**: Streamline interactions with Snowflake from a single local setup.

## Requirements
- Python 3.8+
- [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector-install.html)
- [PyYAML](https://pyyaml.org/)

Install requirements via pip:
```bash
pip install snowflake-connector-python pyyaml pandas
```

## Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/jmfeck/snowflake-local-framework.git
   cd snowflake-local-framework
   ```

2. Configure the YAML files in the `config` directory:
   - **snowflake_conn.yaml**: Connection parameters for Snowflake.
   - **sample_config_ingest.yaml**: Settings for data ingestion.
   - **sample_config_extractor.yaml**: Settings for data extraction.

3. Add SQL queries in the `queries` folder, like `sample_query.sql`, for executing scripts within Snowflake.

## Usage
Run each Python script with the following commands:

- **Ingest Data**: Upload local files to Snowflake.
  ```bash
  python snowflake_ingest_excel.py "config/snowflake_conn.yaml" "config/sample_config_ingest.yaml"
  ```

- **Execute SQL Queries**: Run local SQL scripts within Snowflake.
  ```bash
  python snowflake_query_trigger.py "config/snowflake_conn.yaml" "queries/sample_query.sql"
  ```

- **Extract Data**: Export tables from Snowflake to local files.
  ```bash
  python snowflake_table_extractor.py "config/snowflake_conn.yaml" "config/sample_config_extractor.yaml"
  ```

### Example `.sh` Script for Sequential Execution
Create a Bash script to automate these tasks:
```bash
#!/bin/bash

python snowflake_ingest_excel.py "config/snowflake_conn.yaml" "config/sample_config_ingest.yaml"
python snowflake_query_trigger.py "config/snowflake_conn.yaml" "queries/sample_query.sql"
python snowflake_table_extractor.py "config/snowflake_conn.yaml" "config/sample_config_extractor.yaml"

read -p "Press any key to continue..."
```

Make it executable:
```bash
chmod +x run_snowflake_tasks.sh
```

## Contributing
Contributions are welcome! Please fork the repository and create a pull request for any updates or improvements.

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for more details.

## Contact
Developed by João Manoel Feck  
[Email](mailto:joaomfeck@gmail.com) | [GitHub](https://github.com/jmfeck)
