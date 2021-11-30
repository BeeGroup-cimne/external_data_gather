import argparse

import gemweb
import json
from datetime import datetime
import os
import sys

from Gemweb.gemweb_gather import update_static_data
from utils import connection_mongo

data_types ={
    "entities": {
        "name": "entities",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "entitats"
    },
    "buildings": {
        "name": "buildings",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "centres_consum"
    },
    "solarpv": {
        "name": "solarpv",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "instalacions_solars"
    },
    "supplies": {
        "name": "supplies",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "subministraments"
    },
    "invoices": {
        "name": "invoices",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "factures"
    }
}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--data_type", required=True,
                    help=f"type of data to import: one of {list(data_types.keys())}")
    ap.add_argument("-u", "--data_type", required=True, help="User importing the data")
    args = vars(ap.parse_args())
    with open("./config.json") as config_f:
        config = json.load(config_f)

    if args['data_type'] in data_types.keys():
        mongo = connection_mongo(config['mongo_db'])
        data_source = config['datasources']['gemweb']
        for connection in mongo[data_source['info']].find({}):
            pass
            gemweb.gemweb.connection(connection['username'], connection['password'], timezone="UTC")
            if args['data_type'] == 'invoices':
                try:
                    #data = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="factures",
                    #                                  search_by="factures.data_mod", search_values="2021.06.01",
                    #                                  search_operator=">")

                    update_static_data(data_types[args['data_type']], config['mongo_db'], config['hbase'], connection, data_source, gemweb)
                except Exception as e:
                    print(e)
            else:
                try:
                    update_static_data(data_types[args['data_type']], config['mongo_db'], config['hbase'], connection, data_source, gemweb)
                except Exception as e:
                    print(e)
