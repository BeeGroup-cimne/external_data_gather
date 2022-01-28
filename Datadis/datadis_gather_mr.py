from datetime import datetime, timedelta
import glob
import pickle
import bson
import sys
from abc import ABC

import pandas as pd
import pytz
from mrjob.job import MRJob

from utils import save_to_hbase, mongo_logger, save_to_kafka
from beedis import ENDPOINTS, datadis
from dateutil.relativedelta import relativedelta

TZ = pytz.timezone("Europe/Madrid")

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
        "type_data": "timeseries",
        "freq_rec": relativedelta(months=2),
        "measurement_type": "0",
        "endpoint": ENDPOINTS.GET_CONSUMPTION,
        "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
        "elems_in_day": 24,
    },
    "data_15m": {
        "type_data": "timeseries",
        "freq_rec": relativedelta(days=15),
        "measurement_type": "1",
        "endpoint": ENDPOINTS.GET_CONSUMPTION,
        "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
        "elems_in_day": 96,
    },
    # "max_power": {
    #     "type_data": "timeseries",
    #     "freq_rec": relativedelta(months=6),
    #     "endpoint": ENDPOINTS.GET_MAX_POWER,
    #     "params": ["cups", "distributor_code", "start_date", "end_date"]
    #     "elems_in_day": 1,
    # },
    # "contracts": {
    #     "freq_rec": "static",
    #     "endpoint": ENDPOINTS.GET_CONTRACT,
    #     "params": ["cups", "distributor_code"]
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


def download_chunk(data_type, supply, type_params, credentials, config, status):
    try:
        date_ini_req = status['date_ini_block'].date()
        date_end_req = status['date_end_block'].date()
        sys.stderr.write(f"\t\tObtaining from {date_ini_req} "
                         f"to {date_end_req}\n")
        mongo_logger.log(f"\t\tObtaining from {date_ini_req} "
                         f"to {date_end_req}")

        kwargs = parse_arguments(supply, type_params, date_ini_req, date_end_req)
        consumption = datadis.datadis_query(type_params['endpoint'], **kwargs)
        if not consumption:
            raise GetDataException("No data could be found")

        df_consumption = pd.DataFrame(consumption)
        df_consumption.index = df_consumption['datetime']
        df_consumption.sort_index(inplace=True)
        # Cast datetime64[ns] to timestamp (int64)
        df_consumption['timestamp'] = df_consumption['datetime'].astype('int64') // 10 ** 9

        # send data to kafka or hbase
        save_datadis_data(df_consumption.to_dict('records'), credentials, data_type,
                          ["cups", "timestamp"], [("info", "all")], config, mongo_logger)
        sys.stderr.write(f"\t\t\tRequest sent\n")
        mongo_logger.log(f"\t\t\tRequest sent")
        # store status info
        status['values'] = df_consumption.shape[0]
    except GetDataException as e:
        sys.stderr.write(f"Error gathering data from datadis for user {credentials['username']}: {e}\n")
        mongo_logger.log(f"Error gathering data from datadis for user {credentials['username']}: {e}")
    except Exception as e:
        sys.stderr.write(f"Received and exception: {e}\n")
        mongo_logger.log(f"Received and exception: {e}")


class DatadisMRJob(MRJob, ABC):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_init(self):
        self.__read_config__()

    def mapper(self, _, line):
        credentials = {k: v for k, v in zip(["username", "password", "user", "namespace"], line.split('\t'))}
        mongo_logger.create(self.config['mongo_db'], self.config['datasources']['datadis']['log'], 'gather',
                            user=credentials["user"], datasource_user=credentials["username"],
                            log_exec=datetime.utcnow())
        try:
            mongo_logger.log(f"Login to datadis")
            sys.stderr.write(f"Login to datadis\n")
            login(credentials["username"], credentials["password"])

            # Obtain supplies from the user logged
            mongo_logger.log(f"Obtaining datadis supplies")
            sys.stderr.write(f"Obtaining datadis supplies\n")
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
            for supply in supplies:
                supply.update({"nif": credentials['username']})
            save_datadis_data(supplies, credentials, "supplies", ["cups"], [("info", "all")], self.config, mongo_logger)
            log_exported = mongo_logger.export_log()
            log_exported['log_id'] = str(log_exported['log_id'])
            supplies_by_reducer = 4
            id_key = 0
            mongo_logger.log(f"Obtained: {len(supplies)} supplies")
            sys.stderr.write(f"Obtained: {len(supplies)} supplies\n")
            for i, supply in enumerate(supplies):
                key = f"{credentials['username']}~{id_key}"
                value = {"supply": supply, "credentials": credentials, "logger": log_exported}
                if i % supplies_by_reducer == 0:
                    id_key += 1
                sys.stderr.write(f"Sending: {key}, {value['supply']['cups']}\n")
                yield key, value

        except LoginException as e:
            sys.stderr.write(f"Error in login to datadis for user {credentials['username']}: {e}\n")
            mongo_logger.log(f"Error in login to datadis for user {credentials['username']}: {e}")
        except Exception as e:
            sys.stderr.write(f"Received and exception: {e}\n")
            mongo_logger.log(f"Received and exception: {e}")

    def reducer_init(self):
        self.__read_config__()

    def reducer(self, key, values):
        # Loop supplies
        values = list(values)
        sys.stderr.write(f"Received: {key}, {[x['supply']['cups'] for x in values]}\n")
        for info in values:
            supply = info['supply']
            credentials = info['credentials']
            import_log = info['logger']
            import_log['log_id'] = bson.objectid.ObjectId(import_log['log_id'])
            mongo_logger.import_log(import_log, "gather")
            sys.stderr.write(f"Processing: {supply['cups']}\n")
            mongo_logger.log(f"Processing: {supply['cups']}")
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
                    "_id": supply['cups']
                }
            # create the data chuncks we will gather the information for timeseries
            date_ini = datetime.strptime(supply['validDateFrom'], '%Y/%m/%d').date()
            now = datetime.today().date() + timedelta(days=1)
            try:
                date_end = datetime.strptime(supply['validDateTo'], '%Y/%m/%d').date()
                if date_end <= now:
                    date_end = date_end
                    finished = True
                else:
                    raise Exception()
            except Exception as e:
                date_end = now
                finished = False

            for t, type_params in [(x, y) for x, y in data_types_dict.items() if y["type_data"] == "timeseries"]:
                if t not in device:
                    device[t] = {}
                loop_date_ini = date_ini
                while loop_date_ini < date_end:
                    current_ini = loop_date_ini
                    if finished:
                        current_end = min(date_end, current_ini + type_params['freq_rec'])
                    else:
                        current_end = current_ini + type_params['freq_rec']
                    k = "~".join([current_ini.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")])
                    date_ini_block = datetime.combine(current_ini, datetime.min.time())
                    date_end_block = datetime.combine(current_end, datetime.min.time())
                    time_diff = TZ.localize(date_end_block) - TZ.localize(date_ini_block)
                    if k not in device[t]:
                        device[t].update({
                            k: {
                                "date_ini_block": date_ini_block,
                                "date_end_block": date_end_block,
                                "values": 0,
                                "total": (time_diff.days + 1) * type_params['elems_in_day'] + time_diff.seconds // 3600,
                            }
                        })
                    loop_date_ini = current_end + relativedelta(days=1)
            try:
                login(username=credentials['username'], password=credentials['password'])
            except LoginException as e:
                sys.stderr.write(f"Error in login to datadis for user {credentials['username']}: {e}\n")
                mongo_logger.log(f"Error in login to datadis for user {credentials['username']}: {e}")
            for data_type, type_params in data_types_dict.items():
                sys.stderr.write(f"\tType: {data_type}\n")
                mongo_logger.log(f"\tType {data_type}")
                if type_params['type_data'] == "timeseries":
                    if self.config['policy'] == "last":
                        try:
                            # get last chunk
                            status = list(device[data_type].values())[-1]
                        except IndexError as e:
                            continue
                        # check if chunk is in current time
                        if not(status['date_ini_block'] <= datetime.combine(now, datetime.min.time())
                               <= status['date_end_block']):
                            continue
                        download_chunk(data_type, supply, type_params, credentials, self.config, status)
                        self.increment_counter('gathered', 'device', 1)
                    if self.config['policy'] == "repair":
                        # get all incomplete chunks
                        status_list = [x for x in device[data_type].values() if x['values'] < x['total']]
                        for status in status_list:
                            download_chunk(data_type, supply, type_params, credentials, self.config, status)
                            self.increment_counter('gathered', 'device', 1)

            sys.stderr.write(f"finished device\n")
            mongo_logger.log(f"finished device")
            datadis_devices.replace_one({"_id": device['_id']}, device, upsert=True)
            self.increment_counter('gathered', 'device', 1)


if __name__ == '__main__':
    DatadisMRJob.run()
