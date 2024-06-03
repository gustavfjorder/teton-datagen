import psycopg2
from datetime import datetime, timedelta
import json
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(filePath):
    with open(filePath, 'r') as file:
        return json.load(file)

def updateLastUpdated(filePath, endDate):
    timestamp_str = endDate.isoformat()
    with open(filePath, 'w') as file:
        file.write(timestamp_str)

def getLastUpdated(filePath):
    with open(filePath, 'r') as file:
        date_string = file.read().strip()
    return datetime.fromisoformat(date_string)

def getTablesToDatagen(filePath):
    with open(filePath, 'r') as file:
        return json.load(file)

def datagen(table, startDate, endDate, config):
    # Predefined queries
    fetchQuery = "SELECT * FROM PUBLIC.{0} where {1} between '{2}' and '{3}';"
    getMaxIdQuery = "SELECT MAX(ID) FROM PUBLIC.{0};"

    # Database connection parameters
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
                    cur.execute(insertQuery, modifiedRow)
                    logging.info(f"Inserted row: {modifiedRow} into table: {table['name']}")
                    rowsWritten += 1

                conn.commit()
                print("Rows written: ", rowsWritten, " into table: ", table["name"])
    except Exception as e:
        logging.error(f"Error processing table {table['name']}: {e}")

if __name__ == "__main__":
    config = load_config('resources/config.json')
    tablesToDatagen = getTablesToDatagen('resources/tables_to_datagen.json')

    # We want to fill rows for the interval [lastUpdated, now]
    # That means we fetch rows for the interval [lastUpdated-60d, now-60d] and add 60 days to all timestamps
    lastUpdated = getLastUpdated('resources/last_updated.txt')
    startDate = lastUpdated - timedelta(days=60)
    currentDate = datetime.now()
    endDate = currentDate - timedelta(days=60)

    for table in tablesToDatagen:
        datagen(table, startDate, endDate, config)

    updateLastUpdated('resources/last_updated.txt', currentDate)