import pandas
from sqlalchemy import create_engine
import psycopg2
from bokeh.plotting import figure, output_file, show
from bokeh.tile_providers import CARTODBPOSITRON, get_provider
from bokeh.io import show
from bokeh.models import ColumnDataSource, DataTable, DateFormatter, TableColumn

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
#show(data_table)