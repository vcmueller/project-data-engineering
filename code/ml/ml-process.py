from datetime import timedelta, datetime
import time
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from prefect.executors import LocalDaskExecutor
import psycopg2
import pandas
from sklearn.cluster import KMeans
import dbconfig
from sqlalchemy import create_engine
import dbstatus

def createTable():
    '''
    Create the tables required by the ML process
        - temperature_level to store cluster results
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Create table to store the temperature level data and delete any previous data
    sql = """
        create table IF NOT EXISTS temperature_level (region varchar(50), country varchar(30), city varchar(50), quarter int, avgtemp decimal, templevel varchar(10));
        delete from temperature_level;
        """
    
    # Execute the SQL statement + commit or rollback
    try:
        mycursor.execute(sql)
        connection.commit()
    except:
        connection.rollback()
    
    # Close connection
    connection.close()
    
@task(max_retries=3, retry_delay=timedelta(seconds=1))
def extract():
    '''
    Extract data from temperatures table created by the ETL process
    '''
    # Wait for ETL process to complete
    while (dbstatus.checkStatus(2)):
        print("Waiting for ETL to finish...")
        time.sleep(60)

    # Log status message
    dbstatus.logStatus(3, "ML 1/2 - Process started")

    # Open connection to the database
    alchemyEngine = create_engine(f'postgresql+psycopg2://{dbconfig.USER}:{dbconfig.PASSWORD}@{dbconfig.HOST}/{dbconfig.DBNAME}', pool_recycle=3600);
    connection = alchemyEngine.connect();
   
    # Get the average temperature for each city per quarter
    sql = """select region, country, city, quarter , round( avg(avgtemp),2) as avgtemp
    from temperatures
    group by region, country, city , quarter 
    order by region, country, city, quarter ;
    """
    data = pandas.read_sql_query(sql,con=connection)
    
    # Close database connection
    connection.close()

    # Return queried data
    return data

@task
def createModel(data):
    '''
    Run KMeans to clusterize the data into 5 categories based on their temperature per quarter
    '''
    # Set clusters based on temperature and quarter
    model = KMeans(n_clusters=5, random_state=42).fit(data.drop(columns=["region","country","city"]))
    clusters = model.labels_

    # Add clusters to the data frame as a new column
    data["cluster"] = clusters
    
    # Return the data with the cluster information
    return data

@task
def transform(data):
    ''''
    Transform the clusterized data by 
    - calculating average temperatures
    - setting cluster names based on how low or high the average is
    '''

    # Get average temperature per cluster
    clusterMetadata = pandas.DataFrame({
        "cluster": [0,1,2,3,4],
        "avgtemp":
            [data[data.cluster==0]["avgtemp"].mean(),
            data[data.cluster==1]["avgtemp"].mean(),
            data[data.cluster==2]["avgtemp"].mean(),
            data[data.cluster==3]["avgtemp"].mean(),
            data[data.cluster==4]["avgtemp"].mean()]
            })

    # Set temperature level for each cluster sorted by temperature
    clusterMetadata = clusterMetadata.sort_values(by='avgtemp')
    clusterMetadata['templevel'] = ['Very Low','Low','Medium','High','Very High']

    # Add level to main table
    def setTempLevel(cluster):
        level = clusterMetadata[clusterMetadata.cluster == cluster]['templevel'].iloc[0]
        return str(level)
    data['templevel'] = data.apply(lambda x : setTempLevel(x['cluster']) , axis=1)
    
    # Remove cluster column
    data = data.drop(columns="cluster")
    
    # Return transformed clusterized data
    return data

@task
def load(data):
    ''''
    Task to load the processed data into the database
    '''

    # Connect to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()

    # Iterate through each row and insert to the database
    for index, row in data.iterrows():
        # Prepare SQL query to INSERT a record into the database.
        sql = "INSERT INTO temperature_level (region, country, city, quarter, avgtemp, templevel) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (row[0], row[1], row[2], row[3], row[4], row[5])

        # Execute SQL statement + commit or rollback
        try:
            mycursor.execute(sql)
            connection.commit()
        except:
            connection.rollback()

    # Close database connection
    connection.close()

    # Log status message
    dbstatus.logStatus(4, "ML 2/2 - Process completed")

    return "--- Process successfully completed! ---"

def main():
    '''Set Prefect flow and execute schedule
    NOTE: Schedule can be toggled between test mode and production mode
    Test mode runs every 15 min while production mode runs every quarter
    '''
    # Set Prefect scheduler to run every month
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=15) # Test mode
        #interval=timedelta(days=90) # Production mode
    )

    # Configure Prefect flow
    with Flow("ml", schedule=schedule) as flow:
        data = extract()
        data = createModel(data)
        data = transform(data)
        load(data)

    # Create database tables - if not already created
    createTable()

    # Execute ETL flow
    flow.run(executor=LocalDaskExecutor())

if __name__ == "__main__":
    main()