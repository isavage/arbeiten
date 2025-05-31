import configparser
import csv
import os
import subprocess
from datetime import datetime
from google.cloud import spanner
from google.oauth2 import service_account

# === VARIABLES TO CONFIGURE ===
CONFIG_PATH = "../config/config.ini"
SQL_FILE = "./Spanner.csv.sql"
OUTPUT_DIR = "./output"

# === FUNCTIONS ===

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

def send_email_with_attachments(config, attachments):
    recipient = config['email']['recipient']
    subject = config['email']['subject']
    sender = config['email']['from']

    attachment_flags = ' '.join(f'-a "{file}"' for file in attachments)
    cmd = f"""echo "Attached are the query results." | mailx -s "{subject}" -r "{sender}" {attachment_flags} "{recipient}" """
    print(f"Sending email to {recipient}...")
    subprocess.run(cmd, shell=True, check=True)

def main():
    config = read_config(CONFIG_PATH)
    client = get_spanner_client(config)
    queries = read_sql_file(SQL_FILE)
    results = execute_queries(client, config, queries)
    csv_files = write_results_to_csvs(results)
    send_email_with_attachments(config, csv_files)

if __name__ == "__main__":
    main()
