import configparser
import os
import pandas as pd
import subprocess
from google.cloud import spanner
from google.api_core import exceptions
import logging
from datetime import datetime

# Configuration variables
CONFIG_FILE_PATH = 'config.ini'  # Path to the config file
SQL_FILE_PATH = '/path/to/your/spanner_select_statements.sql'  # Path to the SQL file
OUTPUT_DIR = '/home/user/project/output'  # Directory for CSV files
EMAIL_RECIPIENT = 'recipient@example.com'  # Email recipient
EMAIL_SENDER = 'sender@example.com'  # Email sender (set to '' to omit -r)
EMAIL_SUBJECT = 'Spanner SELECT Results'  # Email subject

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
            'project_id': config['Spanner']['project_id'],
            'instance_id': config['Spanner']['instance_id'],
            'database_id': config['Spanner']['database_id'],
            'credentials_path': config['Spanner']['credentials_path']
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

def save_to_csv(result_rows, query_index):
    """Save query results to a CSV file with date in filename."""
    try:
        if not result_rows:
            logger.info("No rows to save to CSV")
            return None
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(result_rows)
        os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create directory if it doesn't exist
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  # Format: YYYYMMDD_HHMMSS
        csv_filename = os.path.join(OUTPUT_DIR, f"query_result_{query_index}_{timestamp}.csv")
        df.to_csv(csv_filename, index=False)
        logger.info(f"Saved results to {csv_filename}")
        return csv_filename
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def send_email(csv_files, recipient, sender, subject):
    """Send email with CSV files as attachments using Linux mail command."""
    try:
        if not csv_files:
            logger.info("No CSV files to send")
            return
        
        # Prepare mail command with attachments
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        body = f"Spanner SELECT query results generated at {timestamp}"
        mail_cmd = ["mail", "-s", subject]
        
        # Add sender if specified
        if sender:
            mail_cmd.extend(["-r", sender])
        
        # Add attachments
        for csv_file in csv_files:
            mail_cmd.extend(["-a", csv_file])
        
        mail_cmd.append(recipient)
        
        # Send email with body piped in
        process = subprocess.Popen(mail_cmd, stdin=subprocess.PIPE, universal_newlines=True)
        process.communicate(input=body)
        
        if process.returncode == 0:
            logger.info(f"Email sent successfully to {recipient}")
        else:
            raise RuntimeError("Failed to send email via mail command")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise

def execute_select_statements(database, sql_statements):
    """Execute SQL statements in Spanner, save to CSV, and print results."""
    csv_files = []
    for idx, stmt in enumerate(sql_statements, 1):
        try:
            logger.info(f"Executing statement {idx}: {stmt[:100]}...")
            # Execute read-only query using snapshot for consistency
            with database.snapshot() as snapshot:
                try:
                    results = snapshot.execute_sql(stmt)
                except exceptions.InvalidArgument as e:
                    logger.error(f"Invalid query syntax or arguments for statement {idx}: {e}")
                    continue
                except exceptions.NotFound as e:
                    logger.error(f"Table or resource not found for statement {idx}: {e}")
                    continue
                except exceptions.PermissionDenied as e:
                    logger.error(f"Permission denied for statement {idx}: {e}")
                    continue
                except exceptions.GoogleAPIError as e:
                    logger.error(f"Query execution failed for statement {idx}: {e}")
                    continue
                
                if results is None:
                    logger.warning(f"Statement {idx} returned None (invalid query or unexpected result)")
                    continue
                
                # Get column names from metadata.row_type.fields
                column_names = []
                try:
                    if not hasattr(results, 'metadata'):
                        logger.error(f"Statement {idx}: Results metadata missing")
                        continue
                    if not hasattr(results.metadata, 'row_type'):
                        logger.error(f"Statement {idx}: Results row_type missing")
                        continue
                    column_names = [field.name for field in results.metadata.row_type.fields]
                    if not column_names:
                        logger.error(f"Statement {idx}: No column names found in metadata")
                        continue
                except Exception as e:
                    logger.error(f"Failed to extract column names for statement {idx}: {e}")
                    continue
                
                # Convert results to a list of dictionaries
                try:
                    result_rows = [dict(zip(column_names, row)) for row in results]
                except Exception as e:
                    logger.error(f"Error processing result rows for statement {idx}: {e}")
                    continue
                
                # Check if the query has results
                if result_rows:
                    logger.info("Query results:")
                    # Print results to console
                    for row_dict in result_rows:
                        print(row_dict)
                    
                    # Save results to CSV
                    csv_file = save_to_csv(result_rows, idx)
                    if csv_file:
                        csv_files.append(csv_file)
                else:
                    logger.info("No results to display (empty result set)")
                
            logger.info(f"Statement {idx} executed successfully")
        except Exception as e:
            logger.error(f"Unexpected error executing statement {idx}: {e}")
            raise
    
    return csv_files

def main():
    try:
        # Log current working directory
        logger.info(f"Current working directory: {os.getcwd()}")
        
        # Load configuration
        config = load_config(CONFIG_FILE_PATH)
        
        # Use SQL_FILE_PATH variable directly
        sql_file = SQL_FILE_PATH
        
        # Validate SQL file path
        if not sql_file or not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file {sql_file} not found")
        
        # Set up Spanner client
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']
        spanner_client = spanner.Client(project=config['project_id'])
        instance = spanner_client.instance(config['instance_id'])
        database = instance.database(config['database_id'])
        
        # Read and execute SELECT statements
        sql_statements = read_sql_file(sql_file)
        csv_files = execute_select_statements(database, sql_statements)
        
        # Send email with CSV attachments
        send_email(csv_files, EMAIL_RECIPIENT, EMAIL_SENDER, EMAIL_SUBJECT)
        
        logger.info("All operations completed successfully")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
