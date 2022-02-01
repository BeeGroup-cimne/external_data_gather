# Datadis description
Datadis(Datod de distribuïdora) is an application to obtain the energy consumption from the distribution company of spain.

## Raw Data Format
This data source comes in the format of an [API](https://datadis.es/home), where different endpoints are available. In
the followin table we can see the Unique ID for each endpoint.


| Source  | class     | Hbase key     |
|---------|-----------|---------------|
| Datadis | supplies  | cups          |
 | Datadis | data_1h   | cups~time_ini |
| Datadis | data_15m  | cups~time_ini |
| Datadis | max_power | cups~time_ini |

*Mapping key from Datadis source*

## Import script information

For each import run a log document will be stored in mongo:
```json
{
    "user" : "the user that imported data",
    "user_datasource": "the nif of the user importing the data",
    "log_exec" : "The time when the scrip started",
    "logs.gather" : "list with the logs of the import"
}

```
## RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m Datadis -s <storage> -p <policy>
```
