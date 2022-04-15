FROM nginx
COPY . /usr/share/nginx/html
RUN cp /usr/share/nginx/html/nginx/default.conf /etc/nginx/conf.d/default.conf
RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install pip -y
RUN pip install bokeh
RUN pip install psycopg2-binary
RUN pip install pandas
RUN pip install sqlalchemy
#CMD [ "python3 /usr/share/nginx/html/visualization-process.py" ]