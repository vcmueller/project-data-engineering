# Visualization Information

The results of the application can be visualized via `localhost:8080` while the application is running.

All pages are updated every 2 min.

## Home Page

In the home page, it's possible to see links for accessing each one of the tables created by the application.

![HomePage](../../Images/HomePage.png?raw=true "HomePage")


## Process Status

The Process Status page provides information regarding the jobs execution.
The image below shows how the result of the complete flow looks like:

![StatusPage](../../Images/StatusPage.png?raw=true "Status Page")

The column "Last Date Loaded" contains the date of the last record loaded to the database, so subsequent executions should only load newer data.
This page can be used as a monitoring tool as it records all job executions.
- After the status `ETL 7/7` is displayed, the ETL Process page will display the ETL results.
- After the status `ML 2/2` is displayed, the Machine Learning Process page will display the ML results.

## ETL Process

This page shows the table created by the ETL job, which is the transformed data extracted from the source.

![ETLPage](../../Images/ETLPage.png?raw=true "ETL Page")

The temperature data is aggregated by city by month. The result also contains an additional column for quarter.

## Machine Leaning Process

This page shows the table created by the Machine Learning job, which is the ETL resulted data clusterized into groups depending on how the temperature in that city is in that time of the year.

![MLPage](../../Images/MLPage.png?raw=true "ML Page")

Here the temperature is aggregated by city by quarter, and the "Temperature Level" column is the name of the cluster assigned to that city in that quarter.