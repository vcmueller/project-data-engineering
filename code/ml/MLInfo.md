# Machine Learning Job Information

The aim of this job is to clusterize the data resulted from the ETL job by assigning each city and quarter combination to a temperature level.
The temperature level can have a value of `Very Low`, `Low`, `Medium`, `High` or `Very High` and it regards which category that city is in that time of the year by considering the temperature and quarter.

We would expect that the northern hemisphere would have a lower temperature in the 1st and 4th quarters of the year, while the southern hemisphere would be higher. Situation that is then reverted when looking at quarters 2 and 3.

![TemperatureEurope](../../Images/TemperatureEurope.png?raw=true "Temperature Europe")

![TemperatureSouthAmerica](../../Images/TemperatureSouthAmerica.png?raw=true "Temperature SouthAmerica")

The cluster assignment is done based on the average of all temperature data collected for that city aggregated by quarter. The algorithm will re-assign and re-load all data every quarter in order to have an updated version of the clusters.

This cluster information could be used in use cases where we need to identify regions with contain higher/lower temperatures, considering the time of the year, such as ice cream or winter clothing sales.

The ML job uses Prefect to manage the flow and the code is only executed after the ETL job is completed, information that is verified by checking the status table.

## Schedule

The job schedule depends on whether is it in Test Mode or Production Mode.
- Test Mode = job runs every 15 min
- Production Mode = job runs every quarter