# ETL Job Information

The aim of this job is to extract the data from the source, execute a series of transformations so that it's ready to be used by the ML job, then load it to the database.

The resulted table looks like the following:

![TemperatureETL](../../Images/TemperatureETL.png?raw=true "Temperature ETL")

Where the daily temperature records from the source data file are aggregated into month temperature averages.

Observations with missing temperature data are removed.

The `state` column is removed once there is only state information for the US.

A new column `quarter` is added to the table so it can be used by the ML job.

Only unloaded data is loaded to the database. The Status table is used for checking what was the last uploaded data.

In case of incomplete loads caused by failures, then the table is refreshed and all data deleted before a new full load is done.

## Schedule

The job schedule depends on whether is it in Test Mode or Production Mode.
- Test Mode = job runs every 15 min
- Production Mode = job runs every month