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

        # Login User
        try:
            datadis.connection(username=l[0], password=l[1])
            has_datadis_conn = True
        except Exception as ex:
            sys.stderr.write(f"{ex}\n")
            sys.stderr.write(f"{l[0]}\n")

        if has_datadis_conn:
            try:
                # Obtain supplies from the user logged
                supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)

                # Check if the user has supplies
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

        # Loop supplies
        for supply in values:
            has_connection = False

            # Login User
            try:
                datadis.connection(username=supply['user'], password=supply['password'])
                has_connection = True
            except Exception as ex:
                sys.stderr.write(f"{ex}\n")
                sys.stderr.write(f"{supply['user']}\n")

            if has_connection:
                device = datadis_devices.find_one({"cups": supply['cups']})

                # The device exist in our database
                if device:
                    pass

                else:
                    init_date = datetime.datetime.strptime(supply['validDateFrom'], '%Y/%m/%d').date()
                    end_date = datetime.date.today()
                    freq_rec = 4

                    has_data = False
                    last_date = None
                    first_date_init = True
                    first_date = None

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

                        # Consumption has data
                        if consumption:
                            has_data = True
                            # First date gathered
                            if first_date_init:
                                first_date = consumption[0]['datetime']
                                first_date_init = False

                            # Last date gathered
                            last_date = consumption[-1]['datetime']

                        datadis_devices.insert_one({"cups": supply['cups'], "timeToInit": first_date,
                                                    "timeToEnd": last_date,
                                                    "hasError": not has_data, "info": None})

    def mapper_init(self):
        fn = glob.glob('*.pickle')
        config = pickle.load(open(fn[0], 'rb'))
        self.data_type = config['data_type']

    def reducer_init(self):
        fn = glob.glob('*.pickle')
        config = pickle.load(open(fn[0], 'rb'))

        self.hbase = config['hbase']
        self.datasources = config['datasources']
        self.neo4j = config['neo4j']
        self.mongo_db = config['mongo_db']
        self.data_type = config['data_type']


if __name__ == '__main__':
    DatadisMRJob.run()
