# Project: Data Engineering

## Conception Phase

- The batch-processing data application will read data from a CSV file containing city temperatures, clean and transform it (remove nulls, convert units, aggregate) and incrementally load processed data to a PostgreSQL database.
- Prefect will manage the data pipeline flow using Python to execute the ETL tasks, scheduled to run once a month.
- Processed data in the database will be used in another Prefect process to build a Machine Learning model for clusterizing the data, outputting assigned clusters to a new table, scheduled to run once per quarter.
- Bohek will be used to build visualizations, making them available via Nginx.
- PostgreSQL will be accessible to other front-end machine learning applications.
- Four Docker containers will be used to execute the complete flow, using Prefect, PostgreSQL and Nginx images from DockerHub, ensuring maintainability through open source, standarized and well documented tools. The possible deployment to a cluster allows parallelism making it scalable and also reliable by using isolated tasks with dependencies, error handling and docker volumes to persist the data.
- Data security, governance and protection will be ensured via docker networks, only exposing the necessary ports, password-protected access to PostgreSQL, and the deployment of containers to a secure and private/cloud protected server.

### Application Architecture

![Architecture](Architecture/Architecture.png?raw=true "Architecture")

- Details from data source: [source](https://academic.udayton.edu/kissock/http/Weather/source.htm)
- Kaggle link: [city_temperature.csv](https://www.kaggle.com/datasets/sudalairajkumar/daily-temperature-of-major-cities?select=city_temperature.csv)