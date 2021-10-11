import json
import pandas as pd
import os
import sys
sys.path.append(os.getcwd())
from utils import *


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
    if data_type == "contracts":
        res = conn["contracts_datadis"].find({},{"_id:0"}, no_cursor_timeout=True)
    elif data_type == "supplies":
        res = conn["supplies_datadis"].find({},{"_id:0"}, no_cursor_timeout=True)
    elif data_type == "max_power":
        res = conn["max_power_datadis"].find({},{"_id:0"}, no_cursor_timeout=True)
    elif data_type == "hourly_consumption":
        res = conn["hourly_consumption_datadis"].find({},{"_id:0"}, no_cursor_timeout=True)
    else:
        res = conn["quarter_hourly_consumption_datadis"].find({},{"_id:0"}, no_cursor_timeout=True)        

    data = []
    for i in res:
        item = {}
        for k in i.keys():
            item[k] = i[k]
        data.append(item)    
    return data


def load_datadis_hbase(data_type):
    config = get_config()
    hbase = connection_hbase(config["hbase"])
    HTable = 'datadis_' + data_type
    htable = get_HTable(hbase, HTable, {"info": {}})
    documents = get_data(data_type)
    if data_type in ["contracts", "supplies"]:
        save_to_hbase(htable, documents, [("info", "all")], row_fields=["u_id","cups"])
    else:
        save_to_hbase(htable, documents, [("info", "all")], row_fields=["cups"])
        

if __name__ == "__main__":
    load_datadis_hbase('contracts')
