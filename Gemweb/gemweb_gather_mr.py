import glob

from mrjob.job import MRJob
import gemweb
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
from utils import *


class Gemweb_gather(MRJob):
    def reducer_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.connection = config['connection']
        self.hbase_conf = config['config']['hbase_connection']
        self.mongo_conf = config['config']['mongo_connection']
        self.data_source = config['config']['data_source']

    def mapper(self, _, device):
        frequencies = ['data_15m', 'data_1h', 'data_daily', 'data_month']
        for k in frequencies:
            yield "{}~{}".format(device, k), None

    def reducer(self, launch, _):
        device, freq = launch.split("~")
        # call mapreduce with input file as the supplies and performs the following job
        gemweb.gemweb.connection(self.connection['username'], self.connection['password'], timezone="UTC")
        update_info = {"$set": {}}
        frequencies = {
                          'data_15m': {'freq': 'quart-horari', 'step': relativedelta(minutes=15)},
                          'data_1h': {'freq': 'horari', 'step': relativedelta(hours=1)},
                          'data_daily': {'freq': 'diari', 'step': relativedelta(days=1)},
                          'data_month': {'freq': 'mensual', 'step': relativedelta(months=1)}
        }

        user = self.connection['user']
        try:
            data = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=device,
                                              date_from=datetime(2019, 1, 1),
                                              date_to=datetime.now(),
                                              period=frequencies[freq]['freq'])
            self.increment_counter(device, "gathered", 1)
        except:
            data = []
        update_info['$set']["timeseries.{}.{}.updated".format(device, freq)] = datetime.utcnow()

        if len(data) > 0:
            for d in data:
                d['building'] = device
                d['measurement_start'] = int(d['datetime'].timestamp())
                d['measurement_end'] = int((d['datetime'] + frequencies[freq]['step']).timestamp())
            # save obtained data to hbase
            hbase = connection_hbase(self.hbase_conf)
            htable = get_HTable(hbase, "{}_{}_{}".format(self.data_source["hbase_name"], freq, user), {"v": {}, "info": {}})
            save_to_hbase(htable, data, [("v", ["value"]), ("info", ["measurement_end"])], row_fields=['building', 'measurement_start'])
            self.increment_counter(device, "saved", 1)

            update_info['$set']["timeseries.{}.{}.datetime_to".format(device, freq)] = data[-1]['datetime']

            add_d_from = True
            if 'timeseries' in self.connection:
                if device in self.connection['timeseries']:
                    if freq['name'] in self.connection['timeseries'][device]:
                        if 'datetime_from' in self.connection['timeseries'][device][freq]:
                            if self.connection['timeseries'][device][freq] < data[0]['datetime']:
                                add_d_from = False
            if add_d_from:
                update_info['$set']["timeseries.{}.{}.datetime_from".format(device, freq)] = data[0][
                            'datetime']
        else:
            update_info['$set']["timeseries.{}.{}.error".format(device, freq)] = "no new data"

        mongo = connection_mongo(self.mongo_conf)
        mongo[self.data_source['info']].update_one({"_id": self.connection["_id"]}, update_info)
        self.increment_counter(device, "finished", 1)


if __name__ == '__main__':
    Gemweb_gather.run()
