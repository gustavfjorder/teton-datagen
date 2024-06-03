import psycopg2
from datetime import datetime, timedelta
import json
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

def load_config(filePath):
    """Load JSON configuration from a file."""
    try:
        with open(filePath, 'r') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Failed to load config file {filePath}: {e}")
        raise

def getTablesToDatagen(filePath):
    """Load the list of tables to generate data for from the JSON file."""
    try:
        with open(filePath, 'r') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Failed to load tables_to_datagen file {filePath}: {e}")
        raise

def deleteDate(table, dateToDeleteFrom, config):
    """Delete data for the specified table."""

    deleteQuery = "DELETE FROM PUBLIC.{0} where {1} >='{2}';"
    db_params = config['database']

    try:
        # Establish a connection
        with psycopg2.connect(dbname=db_params['dbname'], user=db_params['user'], password=db_params['password'], host=db_params['host'], port=db_params['port']) as conn:
            with conn.cursor() as cur:
                logging.info("Connected to database")

                query = deleteQuery.format(table["name"], table["timestampColumn"], dateToDeleteFrom)
                cur.execute(query)
                conn.commit()
                logging.info(f"Deleted rows since {dateToDeleteFrom} from {table['name']}")
    except Exception as e:
        logging.error(f"Error deleting from table {table['name']}: {e}")

if __name__ == "__main__":
    try:
        config = load_config(os.path.join(script_dir,'config.json'))
        tablesToDatagen = getTablesToDatagen(os.path.join(script_dir,'tables_to_datagen.json'))
        dateToDeleteFrom = datetime(2024, 6, 1)

        for table in tablesToDatagen:
            deleteDate(table, dateToDeleteFrom, config)

    except Exception as e:
        logging.error(f"Failed to run the delete data process: {e}")
