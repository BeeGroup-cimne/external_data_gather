from utils import connection_hbase, get_HTable, save_to_hbase, connection_mongo
from DadesObertes.CEEE.client import CEEE
import json


if __name__ == '__main__':

    df = CEEE().query()

    with open("./config.json") as config_f:
        config = json.load(config_f)

    data_source = config['datasources']['CEEE']

    mongo = connection_mongo(config['mongo_db'])
    info = mongo[data_source['info']].find_one({})
    version = info['version'] + 1
    user = info['user']

    hbase = connection_hbase(config['hbase'])
    htable = get_HTable(hbase, "{}_{}_{}".format(data_source["hbase_name"], "buildings", user), {"info": {}})
    save_to_hbase(htable, df.to_dict("records"), [("info", "all")], row_fields=['Num_Ens_Inventari'], version=version)
