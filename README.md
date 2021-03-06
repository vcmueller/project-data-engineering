# Project: Data Engineering

The project consists of a batch-processing data application for a data-intensive machine learning application that:
- reads data from a set of CSV files containing daily temperature data for cities around the world ([city_temperature.csv](https://www.kaggle.com/datasets/sudalairajkumar/daily-temperature-of-major-cities?select=city_temperature.csv) from Kaggle) with 2.9 million rows;
- processes the source data by aggregating temperatures to a monthly average per city;
- incrementally loads the processed data to a database;
- reads the processed data and adds a label to each record indicating the temperature level of each city on each quarter by using a KMeans Machine Learning model to clusterize the data;
- loads the clusterized data to the database every quarter;
- presents the processed and clusterized data in HTML web pages.
-----------

## Application Architecture

The application architecture is presented below:

![Architecture](Images/Architecture.png?raw=true "Architecture")

### Architecture Implementation

The application is implemented by deploying four docker containers:
- <b>ETL</b>: Prefect is used to manage the data pipeline flow and execute the ETL tasks written in Python ([see code and details here](code/etl)). This job is scheduled to incrementally add new data by running every month.
- <b>Database</b>: PostgreSQL database is used with a docker named volume to persist data outside the container, with port 5432 exposed for external access.
- <b>ML</b>: Prefect is used to manage the flow of extracting the processed data and running the Machine Learning model to clusterize the data, also written in Python ([see code and details here](code/ml)). The process is scheduled to label the data and recreate the table every quarter.
- <b>Visualization</b>: Prefect is used to manage the creation flow of the HTML pages containing the processed data from ETL and ML processes, also using Python and the Bokeh library ([see code and details here](code/visualization)). Nginx service is used to expose the pages created through port 80.

### Containers Integration

The project deployment leaverages Infrastructure as Code via docker-compose with set dependencies for each container.
- All containers depend on the successfully initialization of the database container and the ML job is only executed after the ETL job is done.
- A status table is used to control the flow execution between containers.
- A Docker network is set with the containers deployment, allowing the communication between containers.
-------------

## How to Run

### Pre-Requisites
- Docker
- Docker-compose

### Steps

In order to successfully execute the application and verify the results, follow the steps below:
1. Clone the GitHub repository by running the following command in the terminal/command line:
    ```
    git clone https://github.com/vcmueller/project-data-engineering.git
    ```

2. Go into the cloned repository folder:
    ```
    cd project-data-engineering
    ```

    Note: An alternative to steps 1 and 2 is to download the source files [here](https://github.com/vcmueller/project-data-engineering/archive/refs/heads/main.zip), go into the downloaded folder and continue with Step 3 from the terminal.

3. Execute the following docker-compose command to build the docker images:
    ```
    docker-compose build
    ```

4. Execute the following docker-compose command to start the containers:
    ```
    docker-compose up -d
    ```

    The containers execution status can be monitored via `docker ps`

5. Once the containers are initiated and running, open the following link in the browser:
    [`localhost:8080`](http://localhost:8080)

    ![Home Page](Images/HomePage.png?raw=true "Home Page")

6. The results can be verified by following the links within the webpage

    Note that it might take a few minutes until the jobs are completed, so it's recommended to refresh the pages for seeing updated information.

    By default the ETL and ML jobs are executed in test mode (executed every 15 min). Production mode can be enabled by following the steps documented in [ETL](code/etl) and [ML](code/ml).
    
    The "Process Status" page should reflect the jobs execution details, so it can be used as a monitoring tool.
    
    More information regarding the results displayed can be found [here](code/visualization).

7. To stop the application, run the following docker-compose command:
    ```
    docker-compose down
    ```

----------

## List of Available Documentation
- [ETL Job Details](code/etl)
- [ML Job Details](code/ml)
- [Visualization Job Details](code/visualization)

