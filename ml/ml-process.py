from datetime import date, timedelta, datetime
import time
from dateutil.relativedelta import relativedelta
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.executors import LocalDaskExecutor #DaskExecutor
import psycopg2
import pandas
from sklearn.cluster import KMeans
import dbconfig
from sqlalchemy import create_engine
   
def createTable():
    '''
    Create the initial tables required by the ML process
        - temperature_level to store cluster results
    '''
    # Open connection to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()
    
    # Create table to store the temperatures data and delete any previous data
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
    # Get the average temperature for each city per quarter
    #connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    #cursor = connection.cursor()

    alchemyEngine = create_engine('postgresql+psycopg2://etl:etl@db/db', pool_recycle=3600);
    connection = alchemyEngine.connect();
   
    # Get the date of the last temperature loaded
    sql = """select region, country, city, quarter , round( avg(avgtemp),2) as avgtemp
    from temperatures
    group by region, country, city , quarter 
    order by region, country, city, quarter ;
    """
    #data = pandas.read_sql(sql, connection)
    data = pandas.read_sql_query(sql,con=connection)
    
    # Close database connection
    connection.close()

    return data

@task
def createModel(data):

    # Set clusters based on temperature and quarter
    model = KMeans(n_clusters=5, random_state=42).fit(data.drop(columns=["region","country","city"]))
    pred = model.labels_
    data["cluster"] = pred
    
    return data

@task
def transform(data):
    # Get average temperature per cluster
    clusterMetadata = pandas.DataFrame({
        "cluster": [0,1,2,3,4],
        "avgtemp":[data[data.cluster==0]["avgtemp"].mean(),
    data[data.cluster==1]["avgtemp"].mean(),
    data[data.cluster==2]["avgtemp"].mean(),
    data[data.cluster==3]["avgtemp"].mean(),
    data[data.cluster==4]["avgtemp"].mean()]})

    # Set temperature level for each cluster
    clusterMetadata = clusterMetadata.sort_values(by='avgtemp')
    clusterMetadata['templevel'] = ['Very Low','Low','Medium','High','Very High']

    # Add level to main table
    def setTempLevel(cluster):
        level = clusterMetadata[clusterMetadata.cluster == cluster]['templevel'].iloc[0]
        return str(level)
    data['templevel'] = data.apply(lambda x : setTempLevel(x['cluster']) , axis=1)
    data
    # Remove cluster column
    data = data.drop(columns="cluster")
    
    return data

@task
def load(data):
    ''''
    Task to load the processed data into the database
    '''
    #logStatus("Loading started")

    # Connect to the database
    connection = psycopg2.connect(f"host='{dbconfig.HOST}' dbname='{dbconfig.DBNAME}' user='{dbconfig.USER}' password='{dbconfig.PASSWORD}'")
    mycursor = connection.cursor()

    # Iterate through each row and insert to the database
    for index, row in data.iterrows():
        # Prepare SQL query to INSERT a record into the database.
        sql = "INSERT INTO temperature_level (region, country, city, quarter, avgtemp, templevel) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (row[0], row[1], row[2], row[3], row[4], row[5])
        print(sql)
        # Execute SQL statement + commit or rollback
        try:
            mycursor.execute(sql)
            connection.commit()
        except:
            connection.rollback()

    # Close database connection
    connection.close()

    #logStatus("Loading completed")
    #logStatus(f"Loaded {loadCounter} rows", lastLoaded)
    
    return "--- Process successfully completed! ---"

def main():
    
    # Set Prefect scheduler to run every month
    schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(minutes=600),
    )

    # Configure Prefect flow
    with Flow("ml", schedule=schedule) as flow:
        data = extract()
        data = createModel(data)
        data = transform(data)
        result = load(data)
        print(result)

    # Create database tables - if not already created
    createTable()

    # Execute ETL flow
    flow.run(executor=LocalDaskExecutor())#DaskExecutor())

if __name__ == "__main__":
    main()