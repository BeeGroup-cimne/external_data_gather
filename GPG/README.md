# GPG description
GPG(Gestió de Patrimoni de la Generalitat) is an application to manage the inventory of buildings and properties
belonging to the Generalitat de Catalunya. 

## Raw Data Format
This data source comes in the format of an Excel file where each row is the information about a building. 

The key of the file will be made using the unique field `Num_Ens_Inventari`, that will unequivocally identify each 
building. Future changes on the building will override the previous values. The rest of the file columns will be mapped
to the column family `info` with column using the raw format name.

| Source  |  class    | Hbase key          |
|---------|-----------|--------------------|
|  GPG    |  building | Num_Ens_Inventari  |

*Mapping key from GPG source*

## Import script information

For each import run a log document will be stored in mongo:
```json
{
    "user" : "the user that imported data",
    "log_exec" : "The time when the scrip started",
    "logs.gather" : "list with the logs of the import"
}

```


## RUN import application
To run the import application, execute the python script with the following parameters:

```bash
python3 -m GPG -f <gpg file> -n <namespace> -u <user_importing> -s <storage>
```