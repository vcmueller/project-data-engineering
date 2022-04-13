FROM prefecthq/prefect:latest-python3.9
RUN mkdir ml
COPY . ml/
RUN pip install psycopg2-binary
RUN pip install pandas
RUN pip install sqlalchemy
CMD python ml/read.py
