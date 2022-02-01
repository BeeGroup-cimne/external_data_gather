# Gemweb description
Gemweb is an application to manage the invoices and increase energy efficiency of buildings and properties, they provide
an [API](https://github.com/BeeGroup-cimne/beegweb) to obtain the contained data.

## Raw Data Format
This data source is obtained from the API, where we can obtain different inventory entities and timeseries.

Inventory entities will be considered "static data", as they mainly will remain the same.

Each static element has a gemweb internal `id` wich we will use to unequivocally identify the 
element. Future changes on the element will override the previous values. The rest of the file columns for each entity
will be mapped to the column family `info` with column using the raw format name.

For the timeseries type of data, the key document will be a concatenation of the id field and the timestamp or date 
of the element. This data will be appended everytime and we will only update the previous values when some change will
be applied to previous data.

| Source  |  class    | Hbase key          |
|---------|-----------|--------------------|
| gemweb  |  building |        id          |
| gemweb  |  entities |        id          | 
| gemweb  |  supplies |        id          |
| gemweb  |  invoices |      id~d_mod      |
| gemweb  |time-series|    id~timestamp    |

*Mapping keys from Gemweb source*

## Import script information

For each static data import run, the information stored regarding the status of this import will be a document containing the 
following information:
```json
{
    "user" : "the user that imported data",
    "log_exec" : "The time when the scrip started",
    "logs.gather" : "list with the logs of the import"
}
```
For the timeseries, we will contain a document for each imported device and each granularity, the information contained will be:
**USER BANNED**
```json

```

## RUN import application
To run the import application, execute the python script with the following parameters:

**USER BANNED**

[//]: # (```bash)

[//]: # (## import static data)

[//]: # (#python Gemweb/gemweb_gather.py -d <data_type>)

[//]: # (## where data_type can be one of ['entities', 'buildings', 'solarpv', 'supplies', 'invoices'])

[//]: # (#)

[//]: # (## import timeseries)

[//]: # (#python Gemweb/timeseries_gather.py)

[//]: # (```)