import configparser
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from tabulate import tabulate

# Hardcoded arguments
CONFIG_FILE = "../config/config.ini"
QUERY = "SELECT * FROM your_dataset.your_table LIMIT 10"  # Replace with your dataset and table
OUTPUT_FILE = "../output/bigquery_output.txt"
OUTPUT_FORMAT = "txt"  # Options: "csv", "html", or "txt"

def read_config(config_file):
    """Read connection details from a .ini config file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    if 'BigQuery' not in config:
        raise ValueError("Config file missing [BigQuery] section")
    return {
        'project_id': config['BigQuery'].get('project_id'),
        'service_account_file': config['BigQuery'].get('service_account_file')
    }

def query_bigquery():
    try:
        # Read configuration
        config = read_config(CONFIG_FILE)

        # Load service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            config['service_account_file']
        )

        # Initialize BigQuery client
        client = bigquery.Client(project=config['project_id'], credentials=credentials)

        # Execute query
        query_job = client.query(QUERY)
        results = query_job.result()

        # Convert to DataFrame
        df = results.to_dataframe()

        # Save output
        if OUTPUT_FORMAT.lower() == 'csv':
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"Results saved to {OUTPUT_FILE} as CSV")
        elif OUTPUT_FORMAT.lower() == 'html':
            df.to_html(OUTPUT_FILE, index=False, border=1, classes='table table-striped')
            print(f"Results saved to {OUTPUT_FILE} as HTML")
        elif OUTPUT_FORMAT.lower() == 'txt':
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(tabulate(df, headers='keys', tablefmt='plain', showindex=False))
            print(f"Results saved to {OUTPUT_FILE} as formatted text")
        else:
            raise ValueError("Unsupported output format. Use 'csv', 'html', or 'txt'.")

    except Exception as e:
        print(f"Error executing BigQuery query: {e}")
        sys.exit(1)

if __name__ == "__main__":
    query_bigquery()
