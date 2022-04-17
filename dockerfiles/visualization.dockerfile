FROM prefecthq/prefect:latest-python3.9
RUN apt-get update
RUN apt-get install nginx -y
RUN pip install bokeh
RUN pip install psycopg2-binary
RUN pip install pandas
RUN pip install sqlalchemy
COPY code/visualization /usr/share/nginx/html
COPY code/lib /usr/share/nginx/html
RUN cp /usr/share/nginx/html/nginx/default.conf /etc/nginx/conf.d/default.conf
CMD service nginx start && cd /usr/share/nginx/html/ && python /usr/share/nginx/html/visualization-process.py