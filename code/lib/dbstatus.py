import psycopg2
import dbconfig
from datetime import datetime
import time

def logStatus(status, message, lastLoaded=""):
    '''
    Log status information about the current process
    '''
    # Get time now
    now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Insert status info to the database
    if lastLoaded != "":
        sql = f"INSERT INTO status (status, message, timestamp, lastloaded) VALUES ('{status}', '{message}', '{now}', '{lastLoaded}');"
    else: # if no data was loaded, lastLoaded is not added to the database
        sql = f"INSERT INTO status (status, message, timestamp) VALUES ('{status}', '{message}', '{now}');"
    
    # Execute the SQL statement + commit or rollback
    try:
        mycursor.execute(sql)
        connection.commit()
    except:
        connection.rollback()
    
    # Close connection
    connection.close()

def checkStatus(criteria):
    ''''
    Get last status logged
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    cursor = connection.cursor()
    
    # Get the date of the last row loaded
    cursor.execute("select status from status order by id desc limit 1;")
    lastStatus = cursor.fetchone()[0]
    connection.close()
    
    # If it's the first run, set an initial date
    if lastStatus == criteria:
        return False
    else:
        return True