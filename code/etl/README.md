# ETL Job Details

The aim of this job is to extract the data from the source, execute a series of transformations so that it's ready to be used by the ML job, then load it to the database.

The resulted table looks like the following:

![TemperatureETL](../../Images/TemperatureETL.png?raw=true "Temperature ETL")

- Daily temperature records from the source data file are aggregated into monthly averages.

- Observations with missing temperature data are removed.

- The `state` column is removed once there is only state information for the US.

- A new column `quarter` is added to the table so it can be used by the ML job.

- Temperature data is converted from Fahrenheit to Celsius.

Only new data is loaded to the database. The Status table is used for checking what was the last uploaded data.

In case of incomplete loads caused by failures, the table is refreshed and all data deleted before a new full load is done.

## Schedule

The job schedule depends on whether it is in Test Mode or Production Mode.
- Test Mode = job runs every 15 min
- Production Mode = job runs every month

The execution mode can be toggled by changing the `EXECUTION_MODE` variable under `etl` in the [docker-compose.yaml](../../docker-compose.yaml) file

### Test Mode:
```
environment:
    - EXECUTION_MODE=test
```

### Production Mode:
```
environment:
    - EXECUTION_MODE=production
```