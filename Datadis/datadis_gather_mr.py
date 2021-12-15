import datetime
import glob
import pickle

import sys

import pandas as pd
from mrjob.job import MRJob

from utils import connection_hbase, save_to_hbase, connection_mongo
from neo4j import GraphDatabase
from beedis import ENDPOINTS, datadis


class DatadisMRJob(MRJob):

    def mapper(self, _, line):
        # CUPS address postalCode province municipality distributor validDateFrom validDateTo pointType distributorCode
        l = line.split('\t')

        sys.stderr.write(str(l))

        # db = connection_mongo(self.mongo_db)
        # datadis_devices = db['datadis_devices']
        #
        # device = datadis_devices.find_one({"cups": l[0]})
        #
        # if device:
        #
        #     pass
        # else:
        #     datadis.connection(username="", password="")
        #
        #     # "cups": cups,
        #     # "distributorCode": distributor_code,
        #     # "startDate": start_date.strftime("%Y/%m/%d"),
        #     # "endDate": end_date.strftime("%Y/%m/%d"),
        #     # "measurementType": measurement_type,
        #     # "pointType": point_type,
        #     # "authorizedNif": authorized_nif
        #     start_date = datetime.date(2018, 1, 1)
        #     for i in range(pd.date_range(start_date, datetime.date.today(), freq='3M')):
        #         print(i)
        #
        #         res = datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, cups=l[0], distributorCode=l[-1], startDate=i,
        #                                     endDate="",
        #                                     measurementType=self.data_type, pointType=l[-2])
        #
        #     device_start_data = {"cups": l[0], "timeToInit": start_date,
        #                          "timeToEnd": datetime.date.today(),
        #                          "hasError": False, "info": None}
        #
        # driver = GraphDatabase.driver(self.neo4j['uri'], auth=(self.neo4j['user'], self.neo4j['password']))
        # with driver.session() as session:
        #     session.run()

        yield str(l[0]), str(l[1:])

    def reducer(self, key, values):
        pass

    def mapper_init(self):
        fn = glob.glob('*.pickle')
        config = pickle.load(open(fn[0], 'rb'))

        self.hbase = config['hbase']
        self.datasources = config['datasources']
        self.neo4j = config['neo4j']
        self.mongo_db = config['mongo_db']
        self.data_type = config['data_type']

    def reducer_init(self):
        pass


if __name__ == '__main__':
    DatadisMRJob.run()
