import glob

from mrjob.job import MRJob
import gemweb
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
from utils import *


class Gemweb_gather(MRJob):
    def mapper_init(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.connection = config['connection']
        self.hbase_conf = config['config']['hbase_connection']
        self.mongo_conf = config['config']['mongo_connection']
        self.data_source = config['config']['data_source']

    def mapper(self, _, device):
        # call mapreduce with input file as the supplies and performs the following job
        gemweb.gemweb.connection(self.connection['username'], self.connection['password'], timezone="UTC")
        update_info = {"$set": {}}
        frequencies = [{'name': 'data_15m', 'freq': 'quart-horari', 'step': relativedelta(minutes=15)},
                       {'name': 'data_1h', 'freq': 'horari', 'step': relativedelta(hours=1)},
                       {'name': 'data_daily', 'freq': 'diari', 'step': relativedelta(days=1)},
                       {'name': 'data_month', 'freq': 'mensual', 'step': relativedelta(months=1)}]

        user = self.connection['user']
        for freq in frequencies:
            try:
                data = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=device,
                                                  date_from=datetime(2019, 1, 1),
                                                  date_to=datetime.now(),
                                                  period=freq['freq'])
            except:
                data = []
            update_info['$set']["timeseries.{}.{}.updated".format(device, freq['name'])] = datetime.utcnow()

            if len(data) > 0:
                for d in data:
                    d['building'] = device
                    d['measurement_start'] = int(d['datetime'].timestamp())
                    d['measurement_end'] = int((d['datetime'] + freq['step']).timestamp())
                # save obtained data to hbase
                hbase = connection_hbase(self.hbase_conf)
                htable = get_HTable(hbase, "{}_{}_{}".format(self.data_source["hbase_name"], freq['name'], user), {"v": {}, "info": {}})
                save_to_hbase(htable, data, [("v", ["value"]), ("info", ["measurement_end"])], row_fields=['building', 'measurement_start'])

                update_info['$set']["timeseries.{}.{}.datetime_to".format(device, freq['name'])] = data[-1]['datetime']

                if 'datetime_from' not in self.connection['timeseries'][device][freq['name']]:
                    update_info['$set']["timeseries.{}.{}.datetime_from".format(device, freq['name'])] = data[0][
                        'datetime']
            else:
                if not self.connection['timeseries'][device][freq['name']]:
                    update_info['$set']["timeseries.{}.{}.error".format(device, freq['name'])] = "no data"

            mongo = connection_mongo(self.mongo_conf)
            mongo[self.data_source['info']].update_one({"_id": self.connection["_id"]}, update_info)


if __name__ == '__main__':
    Gemweb_gather.run()
