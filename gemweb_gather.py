import gemweb
import json
from datetime import datetime
import pymongo

from utils import save_to_mongo

with open("config.json") as config_f:
    config = json.load(config_f)

cli = pymongo.MongoClient("mongodb://{user}:{pwd}@{host}:{port}/{db}".format(**config['gemweb']))
db = cli[config['gemweb']['db']]
for connection in db['gemweb'].find({}):
    pass

    # gather all documents
    gemweb.gemweb.connection(connection['username'], connection['password'], timezone="UTC")
    entity = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="entitats")
    save_to_mongo(db['entity'], entity, index_field="id")

    building = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="centres_consum")
    save_to_mongo(db['building'], building, index_field="id")

    solar_pv = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="instalacions_solars")
    save_to_mongo(db['solar_pv'], solar_pv, index_field="id")

    supplies = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="subministraments")
    save_to_mongo(db['supplies'], supplies, index_field="id")

    invoices = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_INVENTORY, category="factures")
    save_to_mongo(db['invoices'], invoices, index_field="id")

    for s in supplies[0:100]:
        try:

            data = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=s['_id'],
                                              date_from=datetime(2020, 1, 1),
                                              date_to=datetime.now(), period="horari")

            # for d in data:
            #     d['id'] = s['_id']
            # save_to_mongo(db['data'], data)

        except Exception as e:
            print(s['_id'], " no data")
            data = []


