from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from prefect.executors import LocalDaskExecutor
import psycopg2
import pandas
import dbconfig
import dbstatus
import os

def createTable():
    '''
    Create the initial tables required by the ETL process
        - temperatures table to store the main data
        - status table to store process status
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Create table to store the temperatures data and status information
    sql = """
        create table IF NOT EXISTS temperatures (region varchar(50), country varchar(30), city varchar(50), quarter int, date date, avgtemp decimal );
        create table IF NOT EXISTS status (id SERIAL, status int, message varchar(40), timestamp timestamp, lastloaded date );
        """
    
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
    Get the date of the last loaded temperature and clean incomplete loads
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    cursor = connection.cursor()
    
    # Get the date of the last temperature loaded
    cursor.execute("SELECT MAX(lastloaded) as lastloaded FROM status;")
    lastLoaded = cursor.fetchone()[0]

    # If it's the first run, set an initial date and remove any incomplete loads from temperatures table
    if lastLoaded is None:
        lastLoaded = '1970-01-01'
        # Execute the SQL statement + commit or rollback
        try:
            cursor.execute("delete from temperatures;")
            connection.commit()
        except:
            connection.rollback()
    
    # Close database connection
    connection.close()

    # Return last loaded date
    return lastLoaded

@task(max_retries=3, retry_delay=timedelta(seconds=1))
def extract():
    ''''
    Task to extract the source data from 2 parts of the file city_temperature.csv
    '''
    # Log status message
    dbstatus.logStatus(1, "ETL 1/7 - Extraction started")

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
        "etl/city_temperature-1.csv", 
        dtype=dtype)
    data2 = pandas.read_csv(
        "etl/city_temperature-2.csv", 
        dtype=dtype)

    # Join both parts in the same data frame
    data = pandas.concat([data1, data2])

    # Log status message
    dbstatus.logStatus(1, "ETL 2/7 - Extraction completed")

    # Return data from source
    return data

@task
def transform(data):
    ''''
    Task to transform the source data
    '''
    # Log status message
    dbstatus.logStatus(1, "ETL 3/7 - Transformation started")

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

    # Log status message
    dbstatus.logStatus(1, "ETL 4/7 - Transformation completed")

    # Return transformed data
    return data

@task
def load(data):
    ''''
    Task to load the processed data into the database
    '''
    # Log status message
    dbstatus.logStatus(1, "ETL 5/7 - Loading started")

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

    # Close database connection
    connection.close()

    # Log status message
    dbstatus.logStatus(1, "ETL 6/7 - Loading completed")

    # If nothing was loaded, last loaded date won't be logged
    if loadCounter == 0:
        lastLoaded = ''
    
    # Log status message
    dbstatus.logStatus(2, f"ETL 7/7 - Loaded {loadCounter} rows", lastLoaded)
    
    return "--- Process successfully completed! ---"

def main():
    '''
    Set Prefect flow and execute schedule
    NOTE: Schedule can be toggled between test mode and production mode
    Test mode runs every 15 min while production mode runs every month
    '''
    
    # Get Execution Mode
    interval=timedelta(minutes=15) # Test mode
    if (os.environ['EXECUTION_MODE'] == 'production'):
        interval=timedelta(days=30)  # Production mode

    # Set Prefect scheduler
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=interval
    )

    # Configure Prefect flow
    with Flow("etl", schedule=schedule) as flow:
        data = extract()
        data = transform(data)
        load(data)

    # Create database tables - if not already created
    createTable()

    # Execute ETL flow
    flow.run(executor=LocalDaskExecutor())

if __name__ == "__main__":
    main()