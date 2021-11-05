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


def get_cups_list():
    conn = connection_mongo()
    res = conn["supplies_datadis"].find({}, no_cursor_timeout=True)
    return [i["cups"] for i in res]


def get_data(data_type, cups = None):
    conn = connection_mongo()
    if not cups: 
        res = conn[data_type_source[data_type]].find({},
                                                     no_cursor_timeout=True)
    else:        
        res = conn[data_type_source[data_type]].find({"cups": cups},
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
            i["datetime"] = int(i["datetime"].timestamp())
    return data
    

def load_datadis_hbase_by_cups(data_type):   
    config = get_config()
    cups_list = get_cups_list()
    for cups in cups_list:        
        documents = get_data(data_type,cups)
        print(cups)
        try:
            hbase = connection_hbase(config["hbase"])
            HTable = 'datadis_' + data_type
            htable = get_HTable(hbase, HTable, {"info": {}})
            save_to_hbase(htable,
                          documents,
                          [("info", "all")],
                          row_fields=["cups", "datetime"])

            print('loaded')
        except Exception as e:
            print('ERROR load to hbase:%s' % e)
            

def load_datadis_hbase(data_type):
    documents = get_data(data_type)
    config = get_config()
    hbase = connection_hbase(config["hbase"])
    HTable = 'datadis_' + data_type
    htable = get_HTable(hbase, HTable, {"info": {}})    

    if data_type in ["contracts", "supplies"]:
        save_to_hbase(htable,
                      documents,
                      [("info", "all")],
                      row_fields=["u_id", "cups"])
    else:
        #max_power
        save_to_hbase(htable,
                      documents,
                      [("info", "all")],
                      row_fields=["cups", "date"])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-d",
                    "--data_type",
                    required=True,
                    help="type of data to import: one of {}".format(data_type_source.keys()))
    args = vars(ap.parse_args())
    if args['data_type'] in data_type_source.keys():
        if args['data_type'] in ["hourly_consumption", 
                                 "quarter_hourly_consumption"]:
            load_datadis_hbase_by_cups(args['data_type'])       
        else:
            load_datadis_hbase(args['data_type'])
    else:
        print("Incorrect type")        
           
