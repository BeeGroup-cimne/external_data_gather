from datetime import datetime
import glob
import pickle
import bson
import sys
from abc import ABC

import pandas as pd
from mrjob.job import MRJob
from pytz import utc

from utils import save_to_hbase, mongo_logger, save_to_kafka
from beedis import ENDPOINTS, datadis
from dateutil.relativedelta import relativedelta


class LoginException(Exception):
    pass


class GetDataException(Exception):
    pass


def login(username, password):
    try:
        datadis.connection(username=username, password=password, timeout=100)
    except Exception as e:
        raise LoginException(f"{e}")


def save_datadis_data(data, credentials, data_type, row_keys, column_map, config, logger):
    if config['store'] == "k":
        logger.log(f"sending data to kafka store topic")
        k_store_topic = config["datasources"]["datadis"]["kafka_store_topic"]
        k_harmonize_topic = config["datasources"]["datadis"]["kafka_harmonize_topic"]
        chunks = range(0, len(data), config['kafka_message_size'])
        logger.log(f"sending data to store and harmonizer topics")
        for num, i in enumerate(chunks):
            message_part = f"{num + 1}/{len(chunks)}"
            try:
                logger.log(f"sending {message_part} part")
                chunk = data[i:i + config['kafka_message_size']]
                kafka_message = {
                    "namespace": credentials["namespace"],
                    "user": credentials["user"],
                    "collection_type": data_type,
                    "source": "datadis",
                    "row_keys": row_keys,
                    "logger": logger.export_log(),
                    "message_part": message_part,
                    "data": chunk
                }
                save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                logger.log(f"part {message_part}  sent correctly")
            except Exception as e:
                logger.log(f"error when sending part {message_part}: {e}")
    elif config['store'] == "h":
        logger.log(f"Saving supplies to HBASE")
        try:
            h_table_name = f"{config['datasources']['datadis']['hbase_table']}_{data_type}_{credentials['user']}"
            save_to_hbase(data, h_table_name, config['hbase_imported_data'], column_map,
                          row_fields=row_keys)
            logger.log(f"Supplies saved successfully")
        except Exception as e:
            logger.log(f"Error saving datadis supplies to HBASE: {e}")
    else:
        logger.log(f"store {config['store']} is not supported")


data_types_dict = {
    "data_1h": {
        "freq_rec": 4,
        "measurement_type": "0",
        "endpoint": ENDPOINTS.GET_CONSUMPTION,
        "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"]
    },
    # "data_15m": {
    #     "freq_rec": 1,
    #     "measurement_type": "1",
    #     "endpoint": ENDPOINTS.GET_CONSUMPTION,
    #     "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"]
    # },
    # "max_power": {
    #     "freq_rec": 6,
    #     "endpoint": ENDPOINTS.GET_MAX_POWER,
    #     "params": ["cups", "distributor_code", "start_date", "end_date"]
    # },
    # "contracts": {
    #     "freq_rec": "static",
    #     "endpoint": ENDPOINTS.GET_MAX_POWER,
    #     "params": ["cups", "distributor_code", "start_date", "end_date"]
    # }
}


def parse_arguments(row, type_params, date_ini, date_end):
    arguments = {}
    for a in type_params['params']:
        if a == "cups":
            arguments["cups"] = row["cups"]
        elif a == "distributor_code":
            arguments["distributor_code"] = row['distributorCode']
        elif a == "start_date":
            arguments["start_date"] = date_ini
        elif a == "end_date":
            arguments["end_date"] = date_end
        elif a == "measurement_type":
            arguments["measurement_type"] = type_params['measurement_type']
        elif a == "point_type":
            arguments["point_type"] = str(row["pointType"])
    return arguments


class DatadisMRJob(MRJob, ABC):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_init(self):
        self.__read_config__()

    def mapper(self, _, line):
        sys.stderr.write(f"Recieved: {line}\n")
        credentials = {k: v for k, v in zip(["username", "password", "user", "namespace"], line.split('\t'))}
        mongo_logger.create(self.config['mongo_db'], self.config['datasources']['datadis']['log'], 'gather',
                            user=credentials["user"], datasource_user=credentials["username"])
        try:
            mongo_logger.log("Login to datadis")
            login(credentials["username"], credentials["password"])

            # Obtain supplies from the user logged
            mongo_logger.log("Obtaining datadis supplies")
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
            for supply in supplies:
                supply.update({"nif": credentials['username']})
            # save_datadis_data(supplies, credentials, "supplies", ["cups"], [("info", "all")], self.config, mongo_logger)
            log_exported = mongo_logger.export_log()
            log_exported['log_id'] = str(log_exported['log_id'])
            supplies_by_reducer = 4
            id_key = 0
            sys.stderr.write(f"Obtained: {len(supplies)} supplies\n")
            for i, supply in enumerate(supplies):
                key = f"{credentials['username']}~{id_key}"
                value = {"supply": supply, "credentials": credentials, "logger": log_exported}
                if i % supplies_by_reducer == 0:
                    id_key += 1
                sys.stderr.write(f"Sending: {key}, {value['supply']['cups']}\n")
                yield key, value

        except LoginException as e:
            sys.stderr.write(f"Error in login to datadis for user {credentials['username']}: {e}")
            mongo_logger.log(f"Error in login to datadis for user {credentials['username']}: {e}")
        except Exception as e:
            sys.stderr.write(f"Received and exception: {e}")
            mongo_logger.log(f"Received and exception: {e}")

    def reducer_init(self):
        self.__read_config__()

    def reducer(self, key, values):
        # Loop supplies
        values = list(values)
        sys.stderr.write(f"Recieved: {key}, {[x['supply']['cups'] for x in values]}\n")
        for info in values:
            supply = info['supply']
            credentials = info['credentials']
            import_log = info['logger']
            sys.stderr.write(f"Processing: {supply['cups']}\n")
            import_log['log_id'] = bson.objectid.ObjectId(import_log['log_id'])
            mongo_logger.import_log(import_log, "gather")
            datadis_devices = \
                mongo_logger.get_connection()[self.config['datasources']['datadis']['log_devices']]
            # get the highest page document log
            try:
                device = datadis_devices.find({"_id": supply['cups']}).sort([("page", -1)]).limit(1)[0]
            except IndexError:
                device = None
            if not device:
                # if there is no log document create a new one
                device = {
                    "_id": supply['cups'],
                    "page": 0,
                    "types": {},
                    "requests_log": []
                }
                for t in data_types_dict:
                    device['types'][t] = {
                        "data_ini": None,
                        "data_end": None,
                        "status": "yes"
                    }
            elif len(device['requests_log']) >= 100:
                # if there is one but with more than 100 records, create a new one with higher page
                device['page'] += 1
                device['requests_log'] = []

            request_log = {}
            try:
                login(username=credentials['username'], password=credentials['password'])
                request_log.update({"login": "success"})
                for data_type, type_params in data_types_dict.items():
                    sys.stderr.write(f"\tType: {data_type}\n")
                    mongo_logger.log(f"obtaining {data_type} data from datadis")
                    if type_params['freq_rec'] != "static":
                        mongo_logger.log(f"the policy is {self.config['policy']}")
                        if self.config['policy'] == "last" and device['types'][data_type]['status'] == "no":
                            sys.stderr.write(f"\t\tIgnoring\n")
                            continue

                        date_end = datetime.today().date()
                        if self.config['policy'] == "last" and device['data_end'] is not None:
                            date_ini = device['data_end']
                        else:
                            date_ini = datetime.strptime(supply['validDateFrom'], '%Y/%m/%d').date()
                        sys.stderr.write(f"\t\tObtaining from {date_ini}\n")
                        # Obtain data
                        while date_ini < date_end:
                            current_date = min(date_end, date_ini + relativedelta(months=type_params['freq_rec']))
                            request_log.update({"date_from": datetime.combine(date_ini, datetime.min.time()),
                                                "date_to": datetime.combine(current_date, datetime.min.time()),
                                                "data_type": data_type})
                            try:
                                kwargs = parse_arguments(supply, type_params, date_ini, current_date)
                                sys.stderr.write(f"\t\t\tRequest from {date_ini} to {current_date}\n")
                                #consumption = datadis.datadis_query(type_params['endpoint'], **kwargs)
                                # if not consumption:
                                #     device['types'][data_type]['status'] = "no"
                                #     raise Exception("No data could be found")
                            except Exception as e:
                                raise GetDataException(f"{e}")
                            request_log.update({"data_gather": "success"})
                            # df_consumption = pd.DataFrame(consumption)
                            # Cast datetime64[ns] to timestamp (int64)
                            # df_consumption['timestamp'] = df_consumption['datetime'].astype('int64') // 10**9
                            # get first and last time gathered
                            # if device['types'][data_type]['data_ini']:
                            #     device['types'][data_type]['data_ini'] = \
                            #         min(device['types'][data_type]['data_ini'], consumption[0]['datetime'])
                            # else:
                            #     device['types'][data_type]['data_ini'] = consumption[0]['datetime']
                            #
                            # if device['types'][data_type]['data_end']:
                            #     device['types'][data_type]['data_end'] = \
                            #         max(device['types'][data_type]['data_end'], consumption[-1]['datetime'])
                            # else:
                            #     device['types'][data_type]['data_end'] = consumption[-1]['datetime']

                            # device['types'][data_type]['status'] = "yes"
                            # save_datadis_data(df_consumption.to_dict('records'), credentials, data_type,
                            #                   ["cups", "timestamp"], [("info", "all")], self.config, mongo_logger)
                            # request_log.update({"sent": "success"})
                            sys.stderr.write(f"\t\t\tRequest sent\n")
                            self.increment_counter('gathered', 'device', 1)
                            date_ini = current_date

                    else:
                        request_log.update({"data_type": data_type})
                        try:
                            kwargs = parse_arguments(supply, type_params, None, None)
                            sys.stderr.write(f"\t\t\tRequest")
                            #data = datadis.datadis_query(type_params['endpoint'], **kwargs)
                            # if not data:
                            #     device['types'][data_type]['status'] = "no"
                            #     raise Exception("No data could be found")
                        except Exception as e:
                            raise GetDataException(f"{e}")
                        request_log.update({"data_gather": "success"})
                        # for d in data:
                        #     d.update({"nif": credentials['username']})
                        device['types'][data_type]['status'] = "yes"
                        # save_datadis_data(data.to_dict('records'), credentials, data_type,
                        #                   ['cups', 'nif'], [("info", "all")], self.config, mongo_logger)
                        sys.stderr.write(f"\t\t\tRequest sent")
                        self.increment_counter('gathered', 'device', 1)
                        request_log.update({"sent": "success"})

            except LoginException as e:
                sys.stderr.write(f"Error in login to datadis for user {credentials['username']}: {e}")
                request_log.update({"login": "fail"})
                mongo_logger.log(f"Error in login to datadis for user {credentials['username']}: {e}")
            except GetDataException as e:
                sys.stderr.write(f"Error gathering data from datadis for user {credentials['username']}: {e}")
                request_log.update({"data_gather": "fail"})
                mongo_logger.log(f"Error gathering data from datadis for user {credentials['username']}: {e}")
            except Exception as e:
                sys.stderr.write(f"Received and exception: {e}")
                mongo_logger.log(f"Received and exception: {e}")

            device['requests_log'].insert(0, request_log)
            datadis_devices.replace_one({"_id": supply['cups'], "page": device['page']}, device,
                                        upsert=True)
            self.increment_counter('gathered', 'device', 1)


if __name__ == '__main__':
    DatadisMRJob.run()
