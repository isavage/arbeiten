import configparser
import csv
import os
import subprocess
from google.cloud import spanner
from google.oauth2 import service_account

def read_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
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
        for query in queries:
            print(f"Executing query: {query}")
            result = snapshot.execute_sql(query)
            columns = result.metadata.row_type.fields
            rows = list(result)
            results.append((columns, rows))
    return results

def write_to_csv(results, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        for idx, (columns, rows) in enumerate(results):
            if idx > 0:
                writer.writerow([])  # blank line between query results
            headers = [field.name for field in columns]
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)

def send_email(config, csv_file):
    recipient = config['email']['recipient']
    subject = config['email']['subject']
    sender = config['email']['from']

    cmd = f"""echo "Attached are the query results." | mailx -s "{subject}" -a "{csv_file}" -r "{sender}" "{recipient}" """
    print(f"Sending email with: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def main():
    config = read_config()
    client = get_spanner_client(config)
    queries = read_sql_file("queries.sql")
    results = execute_queries(client, config, queries)
    output_file = "spanner_output.csv"
    write_to_csv(results, output_file)
    send_email(config, output_file)

if __name__ == "__main__":
    main()
