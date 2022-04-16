from datetime import date, timedelta, datetime
import time
from dateutil.relativedelta import relativedelta
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.executors import LocalDaskExecutor #DaskExecutor
import psycopg2
import pandas
import dbconfig

def createTable():
    '''
    Create the initial tables required by the ETL process
        - temperatures table to store the main data
        - status table to store process status and log
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Create table to store the temperatures data and loading status information
    sql = """
        create table IF NOT EXISTS temperatures (region varchar(50), country varchar(30), city varchar(50), quarter int, date date, avgtemp decimal );
        create table IF NOT EXISTS status (id SERIAL, status int, message varchar(30), timestamp timestamp, lastloaded date );
        """
    
    # Execute the SQL statement + commit or rollback
    try:
        mycursor.execute(sql)
        connection.commit()
    except:
        connection.rollback()
    
    # Close connection
    connection.close()

def logStatus(status, message, lastLoaded=""):
    '''
    Log status information about the ETL process
    '''
    # Get time now
    now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Insert status info to the database
    if lastLoaded != "":
        sql = f"INSERT INTO status (status, message, timestamp, lastloaded) VALUES ('{status}', '{message}', '{now}', '{lastLoaded}');"
    else: # if no data was loaded yet, lastLoaded is not added to the database
        sql = f"INSERT INTO status (status, message, timestamp) VALUES ('{status}', '{message}', '{now}');"
    
    # Execute the SQL statement + commit or rollback
    try:
        mycursor.execute(sql)
        connection.commit()
    except:
        connection.rollback()
    
    # Close connection
    connection.close()

def getLastDateLoaded():
    ''''
    Get the date of the last loaded temperature
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    cursor = connection.cursor()
    
    # Get the date of the last temperature loaded
    cursor.execute("SELECT MAX(lastloaded) as lastloaded FROM status;")
    lastLoaded = cursor.fetchone()[0]

    # If it's the first run, set a initial date
    if lastLoaded is None:
        lastLoaded = '1970-01-01'
    
    # Close database connection
    connection.close()

    return lastLoaded

@task(max_retries=3, retry_delay=timedelta(seconds=1))
def extract():
    ''''
    Task to extract the source data from 2 parts of the file city_temperature.csv
    '''
    logStatus(1, "Extraction started")

    # Set dataframe datatypes
    dtype={ "Region": "string", 
            "Country": "string", 
            "State": "string", 
            "City": "string", 
            "Month": int, 
            "Day": int, 
            "Year": int, 
            "AvgTemperature": float}

    # Read both parts of the file
    data1 = pandas.read_csv(
        "etl/data/city_temperature-1.csv", 
        dtype=dtype)
    data2 = pandas.read_csv(
        "etl/data/city_temperature-2.csv", 
        dtype=dtype)

    # Join both parts in the same data frame
    data = pandas.concat([data1, data2])

    logStatus(1, "Extraction completed")

    return data


@task
def transform(data):
    ''''
    Task to transform the data
    '''
    logStatus(1, "Transformation started")

    # Remove -99 temperatures as it indicates data is not available
    data = data.drop(data[data.AvgTemperature == -99].index)

    # Remove State and Day columns
    data = data.drop(columns=['State', 'Day'])

    # Average temperatures by month rather than days 
    data = data.groupby(['Region','Country','City','Year','Month'], as_index=False).mean()

    # Convert Temperature from F to C
    data['AvgTemperature'] = (data['AvgTemperature'] - 32) * 5 / 9

    # Change temperatures precision to 2 decimal places
    data['AvgTemperature'] = data['AvgTemperature'].round(decimals = 2)

    # Add a column for Quarter
    def setQuarter(month):
        quarter = 0
        if month <= 3:
            quarter = 1
        elif month <= 6:
            quarter = 2
        elif month <= 9:
            quarter = 3
        elif month <= 12:
            quarter = 4
        return quarter
    data['Quarter'] = data.apply(lambda x : setQuarter(x['Month']) , axis=1)

    # Join Year and Month as a new Date column
    data["Date"] = data["Year"].astype(str) + "-" + data["Month"].astype(str) + "-01"
    data["Date"] = pandas.to_datetime(data["Date"])

    # Remove Year and Month columns
    data = data.drop(columns=['Year', 'Month'])

    # Sort data by Date
    data = data.sort_values(by="Date")

    logStatus(1, "Transformation completed")

    # Return transformed data
    return data


@task
def load(data):
    ''''
    Task to load the processed data into the database
    '''
    logStatus(1, "Loading started")

    # Get last loaded timestamp
    lastLoaded = str(getLastDateLoaded())
    lastLoaded = datetime.strptime(lastLoaded, '%Y-%m-%d')
    
    # Add 1 month to the last loaded date to use as filter for the next load
    lastLoaded = lastLoaded + relativedelta(months=1)
    
    # Filter out already loaded data
    data = data[data.Date >= lastLoaded]
    
    # Connect to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()

    # Add counter to log number of rows loaded
    loadCounter = 0

    # --- Testing only ---
    #testCounter = 0

    # Iterate through each row and insert to the database
    for index, row in data.iterrows():
        # Prepare SQL query to INSERT a record into the database.
        sql = "INSERT INTO temperatures (region, country, city, avgtemp, quarter, date) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (row[0], row[1], row[2], row[3], row[4], row[5])
        
        # Execute SQL statement + commit or rollback
        try:
            mycursor.execute(sql)
            connection.commit()

            # Always keep record of the latest date processed
            if lastLoaded < row[5]:
                lastLoaded = row[5]

            # Count loaded row
            loadCounter = loadCounter+1
        
        except:
            connection.rollback()

        # --- Testing only ---
        #testCounter = testCounter+1
        #if testCounter == 1000:
        #    break

    # Close database connection
    connection.close()

    logStatus(1, "Loading completed")

    # If nothing was loaded, last loaded date won't be logged
    if loadCounter == 0:
        lastLoaded = ''
    
    logStatus(2, f"Loaded {loadCounter} rows", lastLoaded)
    
    return "--- Process successfully completed! ---"

def main():

    # Set Prefect scheduler to run every month
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=600),
    )

    # Configure Prefect flow
    with Flow("etl", schedule=schedule) as flow:
        data = extract()
        data = transform(data)
        result = load(data)
        print(result)

    # Create database tables - if not already created
    createTable()

    # Execute ETL flow
    flow.run(executor=LocalDaskExecutor())#DaskExecutor())

if __name__ == "__main__":
    main()