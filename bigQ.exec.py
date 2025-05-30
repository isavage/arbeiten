import configparser
import os
from google.cloud import bigquery
from google.api_core import exceptions
import logging

# Configuration variables
CONFIG_FILE_PATH = 'config.ini'  # Path to the config file
SQL_FILE_PATH = '/path/to/your/sql_statements.sql'  # Path to the SQL file

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_file):
    """Load configuration from config.ini file."""
    try:
        config = configparser.ConfigParser()
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} not found")
        
        config.read(config_file)
        return {
            'project_id': config['bigquery']['project_id'],
            'credentials_path': config['bigquery']['credentials_path'],
            'sql_file': config['bigquery']['sql_file']
        }
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        raise

def read_sql_file(file_path):
    """Read SQL statements from a file."""
    try:
        with open(file_path, 'r') as file:
            # Split statements by semicolon, remove empty statements
            sql_statements = [stmt.strip() for stmt in file.read().split(';') if stmt.strip()]
        return sql_statements
    except Exception as e:
        logger.error(f"Error reading SQL file {file_path}: {e}")
        raise

def execute_sql_statements(client, sql_statements):
    """Execute SQL statements in BigQuery."""
    for stmt in sql_statements:
        try:
            logger.info(f"Executing statement: {stmt[:100]}...")  # Log first 100 chars
            query_job = client.query(stmt)
            query_job.result()  # Wait for the query to complete
            logger.info("Statement executed successfully")
        except exceptions.GoogleAPIError as e:
            logger.error(f"Error executing statement: {e}")
            raise

def main():
    try:
        # Load configuration
        config = load_config(CONFIG_FILE_PATH)
        
        # Use SQL_FILE_PATH variable, fall back to config if not set
        sql_file = SQL_FILE_PATH if SQL_FILE_PATH else config['sql_file']
        
        # Set up BigQuery client
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']
        client = bigquery.Client(project=config['project_id'])
        
        # Read and execute SQL statements
        sql_statements = read_sql_file(sql_file)
        execute_sql_statements(client, sql_statements)
        
        logger.info("All SQL statements executed successfully")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
