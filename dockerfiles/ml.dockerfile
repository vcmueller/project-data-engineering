FROM prefecthq/prefect:latest-python3.9
RUN mkdir ml
COPY code/ml ml/
COPY code/lib ml/
RUN pip install psycopg2-binary
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install sklearn
CMD python ml/ml-process.py
