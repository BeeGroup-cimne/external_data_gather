from mrjob.job import MRJob
import gemweb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class Gemweb_gather(MRJob):
    def mapper_init(self):
        self.hbase_conf = {}
        self.mongo_conf = {}
        self.connection = {}
        self.data_source = {}

    def mapper(self, _, device):
        # call mapreduce with input file as the supplies and performs the following job
        gemweb.gemweb.connection(self.connection['username'], self.connection['password'], timezone="UTC")

        frequencies = [{'name':'data_15m', 'freq':'quart-horari', 'step': relativedelta(minutes=15)},
                       {'name':'data_1h', 'freq':'horari','step': relativedelta(hours=1)},
                       {'name':'data_daily', 'freq': 'diari', 'step': relativedelta(days=1)},
                       {'name':'data_month', 'freq':'mensual', 'step': relativedelta(months=1)}]

        user = self.connection['user']

        for freq in frequencies:
            try:
                data = gemweb.gemweb.gemweb_query(gemweb.ENDPOINTS.GET_METERING, id_=device,
                                                  date_from=datetime(2019, 1, 1),
                                                  date_to=datetime.now(),
                                                  period=freq['freq'])
                for d in data:
                    d['building'] = device
                    d['measurement_start'] = int(d['datetime'].timestamp())
                    d['measurement_end'] = int(( d['datetime'] + freq['step']).timestamp() )
                hbase = hbase = connection_hbase(hbase_conf)
                htable = get_HTable(hbase, "{}_{}_{}".format(data_source["hbase_name"], freq['name'], user), {"v": {}, "info": {}})
                save_to_hbase(htable, data, [("v", ["value"]), ("info", ["measurement_end"])], row_fields=['building', 'measurement_start'])

            except:
                pass
            #             connection['timeseries'][tname] += 1
            #         except:
            #             pass
            # mongo = connection_mongo(mongo_conf)
            # mongo[data_source['info']].replace_one({"_id": connection["_id"]}, connection)


if __name__ == '__main__':
    MRWordFrequencyCount.run()