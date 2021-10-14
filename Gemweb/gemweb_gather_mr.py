import glob

from mrjob.job import MRJob
import gemweb
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
import pytz
from utils import *


class Gemweb_gather(MRJob):
    def reducer_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.connection = config['connection']
        self.hbase_conf = config['config']['hbase_connection']
        self.mongo_conf = config['config']['mongo_connection']
        self.data_source = config['config']['data_source']

    def reducer_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.connection = config['connection']
        self.hbase_conf = config['config']['hbase_connection']
        self.mongo_conf = config['config']['mongo_connection']
        self.data_source = config['config']['data_source']

    def mapper(self, _, device):
        frequencies = ['data_1h', 'data_daily', 'data_month']  # 'data_15m'
        mongo = connection_mongo(self.mongo_conf)
        for k in frequencies:
            mongo['gemweb_debug_log'].insert_one({"device": device, "freq": k, "logs": ["map"]})
            yield "{}~{}".format(device, k), None

    def reducer(self, launch, _):

        device, freq = launch.split("~")
        # call mapreduce with input file as the supplies and performs the following job
    #     gemweb.gemweb.connection(self.connection['username'], self.connection['password'], timezone="UTC")
        mongo = connection_mongo(self.mongo_conf)
        query_doc = {"device": device, "freq": freq}
        mongo['gemweb_debug_log'].update_one(query_doc, {"$push": {"logs": "reduce"}})
    #     device_mongo = mongo['gemweb_timeseries_info'].find_one({"_id": device})
    #     update_info = {"$set": {}}
    #     frequencies = {
    #                       # 'data_15m': {'freq': 'quart-horari', 'step': relativedelta(minutes=15)},
    #                       'data_1h': {'freq': 'horari', 'step': relativedelta(hours=1), 'part': relativedelta(months=1)},
    #                       'data_daily': {'freq': 'diari', 'step': relativedelta(days=1), 'part': relativedelta(months=6)},
    #                       'data_month': {'freq': 'mensual', 'step': relativedelta(months=1), 'part': relativedelta(months=12)}
    #     }
    #
    #     user = self.connection['user']
    #     date_from = datetime(2021, 1, 1)
    #     date_to = datetime.now()
    #     data_t = []
    #     while date_from < date_to:
    #         mongo['gemweb_debug_log'].update_one({"_id": debug_id.inserted_id}, {"$push": {"logs": f"getting from {date_from} to {date_to}"}})
    #         date_to2 = date_from + frequencies[freq]['part']
    #         try:
    #             mongo['gemweb_debug'].insert_one({"device": device, "date_from": date_from,
    #                                             "date_to": date_to2,
    #                                             "period": frequencies[freq]['freq']})
    #
    #             x2 = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=device,
    #                                             date_from=date_from,
    #                                             date_to=date_to2,
    #                                             period=frequencies[freq]['freq'])
    #             mongo['gemweb_debug_log'].update_one({"_id": debug_id.inserted_id},
    #                                                  {"$push": {"logs": f"succeed from {date_from} to {date_to}"}})
    #         except Exception as e:
    #             mongo['gemweb_debug_log'].update_one({"_id": debug_id.inserted_id},
    #                                                  {"$push": {"logs": f"failed from {date_from} to {date_to}"}})
    #
    #             x2 = []
    #         self.increment_counter('gathered', 'device', 1)
    #         date_from = date_to2 + relativedelta(days=1)
    #         data_t.append(x2)
    #
    #     data = []
    #     for x in data_t:
    #         data.extend(x)
    #
    #     update_info["$set"][f"{freq}.updated"] = datetime.utcnow()
    #
    #     if len(data) > 0:
    #         for d in data:
    #             d['building'] = device
    #             d['measurement_start'] = int(d['datetime'].timestamp())
    #             d['measurement_end'] = int((d['datetime'] + frequencies[freq]['step']).timestamp())
    #         # save obtained data to hbase
    #         hbase = connection_hbase(self.hbase_conf)
    #         htable = get_HTable(hbase, "{}_{}_{}".format(self.data_source["hbase_name"], freq, user), {"v": {}, "info": {}})
    #         mongo['gemweb_debug_log'].update_one({"_id": debug_id.inserted_id},
    #                                              {"$push": {"logs": f"saving to hbase"}})
    #         save_to_hbase(htable, data, [("v", ["value"]), ("info", ["measurement_end"])], row_fields=['building', 'measurement_start'])
    #         self.increment_counter('saved', 'device', 1)
    #
    #         update_info['$set'][f"{freq}.datetime_to"] = data[-1]['datetime']
    #         update_info['$unset'] = {f"{freq}.error": ""}
    #         add_d_from = True
    #         if device_mongo:
    #             if freq in device_mongo:
    #                 if 'datetime_from' in device_mongo[freq]:
    #                     if pytz.UTC.localize(device_mongo[freq]['datetime_from']) < data[0]['datetime']:
    #                         add_d_from = False
    #         if add_d_from:
    #             update_info['$set'][f"{freq}.datetime_from"] = data[0]['datetime']
    #     else:
    #         update_info['$set'][f"{freq}.error"] = "no new data"
    #
    #     mongo = connection_mongo(self.mongo_conf)
    #     mongo_d = {"updated": datetime.now()}
    #     mongo['gemweb_timeseries_info'].update_one({"_id": device}, update_info, upsert=True)
    #     mongo['gemweb_debug_log'].update_one({"_id": debug_id.inserted_id},
    #                                          {"$push": {"logs": f"finish"}})
    #     self.increment_counter("finished", 'device', 1)


if __name__ == '__main__':
    Gemweb_gather.run()
