import configparser
import csv
import os
import subprocess
import logging
from datetime import datetime
from google.cloud import spanner
from google.oauth2 import service_account

# === VARIABLES TO CONFIGURE ===
CONFIG_PATH = "../config/config.ini"
SQL_FILE = "./Spanner.csv.sql"
OUTPUT_DIR = "./output"
EMAIL_RECIPIENT = 'recipient@example.com'  # Email recipient
EMAIL_SENDER = 'sender@example.com'  # Email sender (set to '' to omit -r)
EMAIL_SUBJECT = 'BigQuery SELECT Results'  # Email subject

# === FUNCTIONS ===

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config

def get_spanner_client(config):
    credentials_path = config['Spanner']['service_account_file']
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = spanner.Client(project=config['Spanner']['project_id'], credentials=credentials)
    return client

def read_sql_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    queries = [q.strip() for q in content.split(';') if q.strip()]
    return queries

def execute_queries(client, config, queries):
    instance = client.instance(config['Spanner']['instance_id'])
    database = instance.database(config['Spanner']['database_id'])

    results = []
    with database.snapshot() as snapshot:
        for idx, query in enumerate(queries):
            print(f"Executing query {idx + 1}: {query}")
            result = snapshot.execute_sql(query)
            columns = result.metadata.row_type.fields
            rows = list(result)
            results.append((idx + 1, columns, rows))
    return results

def write_results_to_csvs(results):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_files = []

    for idx, (query_num, columns, rows) in enumerate(results):
        filename = f"query_{query_num}_{timestamp}.csv"
        full_path = os.path.join(OUTPUT_DIR, filename)
        with open(full_path, 'w', newline='') as f:
            writer = csv.writer(f)
            headers = [field.name for field in columns]
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
        csv_files.append(full_path)
        print(f"Saved: {full_path}")
    
    return csv_files

def send_email(csv_files, recipient, sender, subject):
    """Send email with CSV files as attachments using Linux mail command."""
    try:
        if not csv_files:
            logger.info("No CSV files to send")
            return
        
        # Prepare mail command with attachments
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        body = f"BigQuery SELECT query results generated at {timestamp}"
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
        
def main():
    config = read_config(CONFIG_PATH)
    client = get_spanner_client(config)
    queries = read_sql_file(SQL_FILE)
    results = execute_queries(client, config, queries)
    csv_files = write_results_to_csvs(results)
    send_email(csv_files, EMAIL_RECIPIENT, EMAIL_SENDER, EMAIL_SUBJECT)

if __name__ == "__main__":
    main()
