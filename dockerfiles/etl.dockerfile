FROM prefecthq/prefect:latest-python3.9
RUN mkdir etl
COPY code/etl etl/
COPY code/lib etl/
COPY data etl/
RUN pip install psycopg2-binary
RUN pip install pandas
CMD python etl/etl_process.py
