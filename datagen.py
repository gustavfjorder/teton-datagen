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

def updateLastUpdated(filePath, endDate):
    """Update the last updated timestamp in the file."""
    try:
        timestampStr = endDate.isoformat()
        with open(filePath, 'w') as file:
            file.write(timestampStr)
        logging.info(f"Updated last_updated.txt with {endDate}")
    except Exception as e:
        logging.error(f"Failed to update last_updated file {filePath}: {e}")
        raise

def getLastUpdated(filePath):
    """Get the last updated timestamp from the file."""
    try:
        with open(filePath, 'r') as file:
            date_string = file.read().strip()
        return datetime.fromisoformat(date_string)
    except Exception as e:
        logging.error(f"Failed to read last_updated file {filePath}: {e}")
        raise

def getTablesToDatagen(filePath):
    """Load the list of tables to generate data for from the JSON file."""
    try:
        with open(filePath, 'r') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Failed to load tables_to_datagen file {filePath}: {e}")
        raise

def checkQueryLegality(insertQuery, modifiedRow):
    """Check if the query is legal."""
    if "INSERT" in insertQuery.lower():
        for value in modifiedRow:
            if isinstance(value, datetime) and value < datetime(2024, 6, 1):
                logging.error(f"Query: {insertQuery} is illegal for row: {modifiedRow} because it contains a date before 2024-06-01")
                raise Exception(f"Query: {insertQuery} is illegal for row: {modifiedRow} because it contains a date before 2024-06-01")

def datagen(table, startDate, endDate, config):
    """Generate data for the specified table."""

    fetchQuery = "SELECT * FROM PUBLIC.{0} where {1} between '{2}' and '{3}';"
    getMaxIdQuery = "SELECT MAX(ID) FROM PUBLIC.{0};"
    db_params = config['database']

    try:
        # Establish a connection
        with psycopg2.connect(dbname=db_params['dbname'], user=db_params['user'], password=db_params['password'], host=db_params['host'], port=db_params['port']) as conn:
            with conn.cursor() as cur:
                logging.info("Connected to database")
                rowsWritten = 0

                # Fetch rows from 60 days ago and use them as template for new rows
                query = fetchQuery.format(table["name"], table["timestampColumn"], startDate, endDate)
                cur.execute(query)
                rows = cur.fetchall()
                allColumns = ", ".join(table["allColumns"])
                column_indices = {col: i for i, col in enumerate(table["allColumns"])}

                for row in rows:
                    modifiedRow = list(row)

                    # Convert dict to json
                    for i in range(len(modifiedRow)):
                        if isinstance(modifiedRow[i], dict):
                            modifiedRow[i] = json.dumps(modifiedRow[i])
                    
                    # Add 60 days to the columns that need to be datagened
                    for columnToDatagen in table["columnsToDatagen"]:
                        index = column_indices[columnToDatagen]
                        columnValue = modifiedRow[table["allColumns"].index(columnToDatagen)]
                        if isinstance(columnValue, datetime):
                            modifiedRow[index] += timedelta(days=60)
                        else:
                            logging.error("Column: ", columnToDatagen, " is not a datetime object for row: ", row)
                    
                    # Get the maximum value in the ID column, we increment it by 1 to get the new ID
                    cur.execute(getMaxIdQuery.format(table["name"]))
                    maxId = cur.fetchone()[0]
                    modifiedRow[0] = maxId + 1

                    # Insert the new row
                    insertQuery = "INSERT INTO PUBLIC.{0} ({1}) VALUES ({2})".format(table["name"], allColumns, ", ".join(["%s"]*len(table["allColumns"])))
                    checkPreconditions(insertQuery, modifiedRow)
                    cur.execute(insertQuery, modifiedRow)
                    logging.info(f"Inserted row: {modifiedRow} into table: {table['name']}")
                    rowsWritten += 1

                conn.commit()
                print("Rows written: ", rowsWritten, " into table: ", table["name"])
    except Exception as e:
        logging.error(f"Error processing table {table['name']}: {e}")

if __name__ == "__main__":
    try:
        config = load_config(os.path.join(script_dir,'resources/config.json'))
        tablesToDatagen = getTablesToDatagen(os.path.join(script_dir,'resources/tables_to_datagen.json'))

        # We want to fill rows for the interval [lastUpdated, now]
        # That means we fetch rows for the interval [lastUpdated-60d, now-60d] and add 60 days to all timestamps
        lastUpdated = getLastUpdated(os.path.join(script_dir, 'resources/last_updated.txt'))
        startDate = lastUpdated - timedelta(days=60)
        currentDate = datetime.now()
        endDate = currentDate - timedelta(days=60)

        for table in tablesToDatagen:
            datagen(table, startDate, endDate, config)

        updateLastUpdated(os.path.join(script_dir,'resources/last_updated.txt'), currentDate)
    except Exception as e:
        logging.error(f"Failed to run the datagen process: {e}")
