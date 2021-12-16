Application to import data from Mongo to Hbase.

## RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 Datadis/_datadis_gather.py -d <data_type>
# where data_type can be one of ['contracts', 'supplies', 'max_power', 'hourly_consumption', 'quarter_hourly_consumption']

```
