import psycopg2
import pandas
from sqlalchemy import create_engine


#connection = psycopg2.connect("host='db' dbname='db' user='etl' password='etl'")
alchemyEngine = create_engine('postgresql+psycopg2://etl:etl@db/db', pool_recycle=3600);
connection = alchemyEngine.connect();

def readTable():

    df = pandas.read_sql_query('select * from temperatures',con=connection)
    print(df.head())

    connection.close()


if __name__ == "__main__":
    readTable()