import pandas
from sqlalchemy import create_engine
import psycopg2
from bokeh.plotting import figure, output_file, show
from bokeh.tile_providers import CARTODBPOSITRON, get_provider
from bokeh.io import show
from bokeh.models import ColumnDataSource, DataTable, DateFormatter, TableColumn
import time
import dbconfig
from datetime import date, timedelta, datetime
import time
from dateutil.relativedelta import relativedelta
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.executors import LocalDaskExecutor #DaskExecutor

'''
def logStatus(status, message):

    # Get time now
    now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Insert status info to the database
    sql = f"INSERT INTO status (status, message, timestamp) VALUES ('{status}', '{message}', '{now}');"
    
    # Execute the SQL statement + commit or rollback
    try:
        mycursor.execute(sql)
        connection.commit()
    except:
        connection.rollback()
    
    # Close connection
    connection.close()
'''

@task(max_retries=3, retry_delay=timedelta(seconds=1))
def getResult():
    
    # Wait for ML process to complete
    while (checkStatus()):
        print("Not ready -- wait ...")
        time.sleep(120)

    alchemyEngine = create_engine('postgresql+psycopg2://etl:etl@db/db', pool_recycle=3600);
    connection = alchemyEngine.connect();
    
    # Get the date of the last temperature loaded
    sql = """select * from temperature_level ;"""
    #data = pandas.read_sql(sql, connection)
    data = pandas.read_sql_query(sql,con=connection)

    # Close database connection
    connection.close()

    source = ColumnDataSource(data)

    columns = [
            TableColumn(field="region", title="Region"),
            TableColumn(field="country", title="Country"),
            TableColumn(field="city", title="City"),
            TableColumn(field="quarter", title="Quarter"),
            TableColumn(field="avgtemp", title="Temperature"),
            TableColumn(field="templevel", title="Temperature Level"),
        ]
    data_table = DataTable(source=source, columns=columns, width=800, height=800)

    output_file("pages/result.html")
    show(data_table)

@task
def getStatus():
    alchemyEngine = create_engine('postgresql+psycopg2://etl:etl@db/db', pool_recycle=3600);
    connection = alchemyEngine.connect();
    
    # Get the date of the last temperature loaded
    sql = """select * from status ;"""
    #data = pandas.read_sql(sql, connection)
    data = pandas.read_sql_query(sql,con=connection)

    # Close database connection
    connection.close()

    source = ColumnDataSource(data)

    columns = [
            TableColumn(field="id", title="id"),
            TableColumn(field="status", title="status"),
            TableColumn(field="message", title="message"),
            TableColumn(field="timestamp", title="timestamp"),
            TableColumn(field="lastloaded", title="lastloaded"),
        ]
    data_table = DataTable(source=source, columns=columns, width=800, height=800)

    output_file("pages/etlstatus.html")
    show(data_table)

def checkStatus():
    ''''
    Get last status logged
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    cursor = connection.cursor()
    
    # Get the date of the last temperature loaded
    cursor.execute("select status from status order by id desc limit 1;")
    lastStatus = cursor.fetchone()[0]
    connection.close()
    
    # If it's the first run, set a initial date
    if lastStatus == 4:
        return False
    else:
        return True

def main():
    
    # Set Prefect scheduler to run every month
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=5),
    )

    # Configure Prefect flow
    with Flow("etl", schedule=schedule) as flow:
        getResult()
        getStatus()

    # Execute ETL flow
    flow.run(executor=LocalDaskExecutor())#DaskExecutor())

if __name__ == "__main__":
    main()