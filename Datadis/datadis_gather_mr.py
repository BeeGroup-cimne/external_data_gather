import datetime
import glob
import pickle

import sys

import pandas as pd
from mrjob.job import MRJob

from utils import connection_hbase, save_to_hbase, connection_mongo
from neo4j import GraphDatabase
from beedis import ENDPOINTS, datadis
from dateutil.relativedelta import relativedelta


class DatadisMRJob(MRJob):

    def mapper(self, _, line):
        l = line.split('\t')  # [user,password,organisation]
        has_datadis_conn = False

        try:
            datadis.connection(username=l[0], password=l[1])
            has_datadis_conn = True
        except Exception as ex:
            sys.stderr.write(f"{ex}\n")
            sys.stderr.write(f"{l[0]}\n")

        if has_datadis_conn:
            try:
                supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
                if supplies:
                    for supply in supplies[:1]:
                        key = supply['cups']
                        value = supply.copy()
                        value.update({"user": l[0], "password": l[1], "organisation": l[2]})
                        yield key, value

            except Exception as ex:
                sys.stderr.write(f"{ex} : [ {l[0]} ]\n")

    def reducer(self, key, values):
        # Mongo DB
        db = connection_mongo(self.mongo_db)
        datadis_devices = db['datadis_devices']

        for supply in values:
            device = datadis_devices.find_one({"cups": supply['cups']})

            if device:
                pass

            else:
                init_date = datetime.datetime.strptime(supply['validDateFrom'], '%Y/%m/%d').date()
                end_date = datetime.date.today()
                freq_rec = 1

                consumptions = []
                for i in pd.date_range(init_date, end_date, freq=f"{freq_rec}M"):
                    first_date_of_month = i.replace(day=1)
                    final_date = first_date_of_month + relativedelta(months=freq_rec) - datetime.timedelta(
                        days=1)

                    consumption = datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, cups=supply['cups'],
                                                        distributor_code=supply['distributorCode'],
                                                        start_date=first_date_of_month,
                                                        end_date=final_date,
                                                        measurement_type="0",
                                                        point_type=str(supply['pointType']))

                    sys.stderr.write(f"{consumption}\n")
                    
                if consumptions:
                    pass
                else:
                    pass
        # -----------------------------------------------------------------------------------------
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
