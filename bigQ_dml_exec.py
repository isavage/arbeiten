import logging
import configparser
import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(
    filename='bigquery_dml.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config(config_file):
    """
    Load configuration from config.ini file.
    
    Args:
        config_file (str): Path to the configuration file
        
    Returns:
        tuple: project_id, dataset_id, credentials
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        
        project_id = config['BigQuery']['project_id']
        dataset_id = config['BigQuery']['dataset_id']
        auth_json_path = config['BigQuery']['auth_json_path']
        
        # Set credentials from auth JSON file
        if not os.path.exists(auth_json_path):
            raise FileNotFoundError(f"Authentication JSON file not found at: {auth_json_path}")
        credentials = service_account.Credentials.from_service_account_file(auth_json_path)
        
        return project_id, dataset_id, credentials
    
    except KeyError as e:
        logging.error(f"Missing configuration key: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error reading config file '{config_file}': {str(e)}")
        raise

def execute_dml_from_file(config_file, input_file):
    """
    Execute DML commands from an input file and log results.
    
    Args:
        config_file (str): Path to configuration file
        input_file (str): Path to input file containing DML statements
    """
    try:
        # Load configuration
        project_id, dataset_id, credentials = load_config(config_file)
        
        # Initialize BigQuery client with credentials
        client = bigquery.Client(project=project_id, credentials=credentials)
        
        # Read DML statements from file
        with open(input_file, 'r') as file:
            dml_statements = [line.strip() for line in file if line.strip() and not line.startswith('#')]
        
        # Execute each DML statement
        for statement in dml_statements:
            try:
                # Ensure dataset reference in queries
                query = statement.replace('@dataset@', f"{project_id}.{dataset_id}")
                query_job = client.query(query)
                query_job.result()  # Wait for the query to complete
                row_ct = query_job.num_dml_affected_rows
                logging.info(f"Successfully executed: '{statement}' - {row_ct} record(s) affected")
                print(f"Executed: '{statement}' - {row_ct} record(s) affected")
            except GoogleAPIError as e:
                logging.error(f"Failed to execute '{statement}': {str(e)}")
                print(f"Error executing '{statement}': {str(e)}")
                
    except FileNotFoundError as e:
        logging.error(f"Input file '{input_file}' not found")
        print(f"Error: Input file '{input_file}' not found")
    except GoogleAPIError as e:
        logging.error(f"BigQuery client error: {str(e)}")
        print(f"Error initializing BigQuery client: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    config_file = "config.ini"
    input_file = "dml_commands.txt"
    
    execute_dml_from_file(config_file, input_file)
