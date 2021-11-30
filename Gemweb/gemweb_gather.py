import argparse

import gemweb
import json
from datetime import datetime
import os
import sys
sys.path.append(os.getcwd())
from utils import *

BATCH = 1000


def update_static_data(data_type, mongo_conf, hbase_conf, connection, data_source, gemweb):
    version = connection[data_type['name']]['version'] + 1
    user = connection['user']
    print("getting_data")
    data = gemweb.gemweb.gemweb_query(data_type['endpoint'], category=data_type['category'])
    print("uploading_data")
    hbase = hbase = connection_hbase(hbase_conf)
    htable = get_HTable(hbase, "{}_{}_{}".format(data_source["hbase_name"], data_type['name'], user), {"info": {}})
    save_to_hbase(htable, data, [("info", "all")], row_fields=['id'], version=version)

    connection[data_type['name']]['version'] = version
    connection[data_type['name']]['inserted'] = len(data)
    connection[data_type['name']]['date'] = datetime.now()
    mongo = connection_mongo(mongo_conf)
    mongo[data_source['info']].replace_one({"_id": connection["_id"]}, connection)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--data_type", required=True, help="type of data to import: one of {}".format(list(data_types.keys())))
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



