import datetime
import glob
import pickle

import sys

import pandas as pd
from mrjob.job import MRJob
from pytz import utc

from utils import connection_hbase, save_to_hbase, connection_mongo
from beedis import ENDPOINTS, datadis
from dateutil.relativedelta import relativedelta

data_type_source = {"contracts": "contracts_datadis",
                    "supplies": "supplies_datadis",
                    "max_power": "max_power_datadis",
                    "hourly_consumption": "hourly_consumption_datadis",
                    "quarter_hourly_consumption": "quarter_hourly_consumption_datadis"}


def login(username, password):
    try:
        datadis.connection(username=username, password=password)
        return True
    except Exception as ex:
        sys.stderr.write(f"{ex}\n")
        sys.stderr.write(f"{username}\n")

    return False


class DatadisMRJob(MRJob):

    def mapper(self, _, line):
        l = line.split('\t')  # [user,password,organisation]
        has_datadis_conn = login(l[0], l[1])

        if has_datadis_conn:
            try:
                # Obtain supplies from the user logged
                supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)

                # Check if the user has supplies
                if supplies:
                    for supply in supplies:
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
            has_connection = login(username=supply['user'], password=supply['password'])

            if has_connection:
                if self.data_type == "hourly_consumption" or self.data_type == "quarter_hourly_consumption":

                    device = datadis_devices.find_one({"_id": supply['cups']})

                    if self.data_type == "hourly_consumption":
                        freq_rec = 4

                        has_data = False
                        first_date_init = True
                        end_date = datetime.date.today()
                        first_date = None
                        last_date = None

                        # The device exist in our database and is not None
                        if device and device['timeToEnd']:
                            init_date = device['timeToEnd']
                        else:
                            init_date = datetime.datetime.strptime(supply['validDateFrom'], '%Y/%m/%d').date()

                        # Obtain data
                        for i in pd.date_range(init_date, end_date, freq=f"{freq_rec}M", tz='Europe/Madrid'):

                            first_date_of_month = i.replace(day=1)
                            final_date = first_date_of_month + relativedelta(months=freq_rec) - datetime.timedelta(
                                days=1)

                            try:
                                consumption = datadis.datadis_query(ENDPOINTS.GET_CONSUMPTION, cups=supply['cups'],
                                                                    distributor_code=supply['distributorCode'],
                                                                    start_date=first_date_of_month.date(),
                                                                    end_date=final_date.date(),
                                                                    measurement_type="0",  # 0 = hour, 1 = quarter hour
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

                            except Exception as ex:
                                sys.stderr.write(f"{ex}")

                        if device:
                            if has_data:
                                # TODO: Save to Hbase

                                datadis_devices.update_one({"_id": supply['cups']}, {"$set": {
                                    "timeToInit": min(first_date.replace(tzinfo=utc),
                                                      device['timeToInit'].replace(tzinfo=utc)),
                                    "timeToEnd": max(last_date.replace(tzinfo=utc),
                                                     device['timeToEnd'].replace(tzinfo=utc)),
                                    "hasError": not has_data, "info": None}})
                            else:
                                datadis_devices.update_one({"_id": supply['cups']}, {"$set": {
                                    "hasError": not has_data,
                                    "info": f"{datetime.datetime.now().__str__()}: The system didn't find new data."}})
                        else:
                            datadis_devices.insert_one({"_id": supply['cups'], "timeToInit": first_date,
                                                        "timeToEnd": last_date,
                                                        "hasError": not has_data, "info": None})

    def reducer_init(self):
        fn = glob.glob('*.pickle')
        config = pickle.load(open(fn[0], 'rb'))

        self.hbase = config['hbase']
        self.datasources = config['datasources']
        self.neo4j = config['neo4j']
        self.mongo_db = config['mongo_db']
        self.data_type = config['data_type']

    def gather_hourly_consumption(self, supply, device, freq_rec):
        pass

    def gather_quarter_hourly_consumption(self):
        pass


if __name__ == '__main__':
    DatadisMRJob.run()
