import configparser
import sys
from google.cloud import spanner
from google.oauth2 import service_account
import pandas as pd

# Hardcoded arguments
CONFIG_FILE = "config/config.ini"
QUERY = "SELECT * FROM your_table LIMIT 10"  # Replace with your table
OUTPUT_FILE = "output/spanner_output.csv"
OUTPUT_FORMAT = "csv"  # Options: "csv" or "html"

def read_config(config_file):
    """Read connection details from a .ini config file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    if 'Spanner' not in config:
        raise ValueError("Config file missing [Spanner] section")
    return {
        'project_id': config['Spanner'].get('project_id'),
        'instance_id': config['Spanner'].get('instance_id'),
        'database_id': config['Spanner'].get('database_id'),
        'service_account_file': config['Spanner'].get('service_account_file')
    }

def query_spanner():
    try:
        # Read configuration
        config = read_config(CONFIG_FILE)

        # Load service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            config['service_account_file']
        )

        # Initialize Spanner client with explicit credentials
        client = spanner.Client(project=config['project_id'], credentials=credentials)
        instance = client.instance(config['instance_id'])
        database = instance.database(config['database_id'])

        # Execute query
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(QUERY)
            columns = [field.name for field in results.fields]
            rows = [row for row in results]

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Save output
        if OUTPUT_FORMAT.lower() == 'csv':
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"Results saved to {OUTPUT_FILE} as CSV")
        elif OUTPUT_FORMAT.lower() == 'html':
            df.to_html(OUTPUT_FILE, index=False, border=1, classes='table table-striped')
            print(f"Results saved to {OUTPUT_FILE} as HTML")
        else:
            raise ValueError("Unsupported output format. Use 'csv' or 'html'.")

    except Exception as e:
        print(f"Error executing Spanner query: {e}")
        sys.exit(1)

if __name__ == "__main__":
    query_spanner()
