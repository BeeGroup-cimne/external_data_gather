import json
import os
import argparse
import datetime
import sys
sys.path.append(os.getcwd())
from utils import *

data_type_source = {"contracts": "contracts_datadis",
                    "supplies": "supplies_datadis",
                    "max_power": "max_power_datadis",
                    "hourly_consumption": "hourly_consumption_datadis",
                    "quarter_hourly_consumption": "quarter_hourly_consumption_datadis"}


def get_config():
    f = open("Datadis/config.json", "r")
    return json.load(f)


def connection_mongo():
    config = get_config()
    cli = MongoClient("mongodb://{user}:{pwd}@{host}:{port}/{db}".format(
        **config['mongo_db']))
    db = cli[config['mongo_db']['db']]
    return db


def get_data(data_type):
    conn = connection_mongo()
    res = conn[data_type_source[data_type]].find({},
                                                 no_cursor_timeout=True)
    data = []
    for i in res:
        item = {}
        for k in i.keys():
            if k !="_id":                
                item[k] = i[k]

        data.append(item)

    if data_type == "max_power":
        for i in data:
            i["date"] = int(datetime.datetime.strptime(i["date"],"%Y/%m/%d").timestamp())
    elif data_type in ["hourly_consumption","quarter_hourly_consumption"]:
        for i in data:
            i["datetime"] = i["datetime"].timestamp()                
    return data

def split_list(data_list,list_size):
    result = []
    for i in range(0,len(data_list), list_size):
        result.append(data_list[i:i + list_size])
    return result

def load_datadis_hbase(data_type):
    config = get_config()
    hbase = connection_hbase(config["hbase"])
    HTable = 'datadis_' + data_type
    htable = get_HTable(hbase, HTable, {"info": {}})
    documents = get_data(data_type)    

    if data_type in ["contracts", "supplies"]:
        save_to_hbase(htable,
                      documents,
                      [("info", "all")],
                      row_fields=["u_id", "cups"])
    elif data_type == "max_power":
        save_to_hbase(htable,
                      documents,
                      [("info", "all")],
                      row_fields=["cups", "date"])
    else:
        documents_partition = self.split_list(documents,200000)
        for block in documents_partition:
            save_to_hbase(htable,
                          block,
                          [("info", "all")],
                          row_fields=["cups", "datetime"])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-d",
                    "--data_type",
                    required=True,
                    help="type of data to import: one of {}".format(data_type_source.keys()))
    args = vars(ap.parse_args())

    if args['data_type'] in data_type_source.keys():
        load_datadis_hbase(args['data_type'])
