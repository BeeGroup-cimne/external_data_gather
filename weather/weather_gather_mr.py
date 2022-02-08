from datetime import datetime, timedelta
import glob
import pickle
import bson
import sys
from abc import ABC

from mrjob.job import MRJob

from utils import mongo_logger, save_to_kafka
from dateutil.relativedelta import relativedelta
from beemeteo.sources.darksky import DarkSky


def download_chunk(cp, type_params, config, date_ini, date_end):
    source = type_params['source'](config)
    data = source.get_historical_data(
        float(cp['latitude']),
        float(cp['longitude']),
        date_ini,
        date_end
    )
    return data


def save_weather_data(data, logger, config):
    kafka_message = {
        "namespace": "https://weather.beegroup-cimne.com#",
        "user": "BEEgroup",
        "collection_type": "darksky",
        "source": "weather",
        "logger": logger.export_log(),
        "data": data
    }
    save_to_kafka(topic=config['datasources']['weather']['kafka_harmonize_topic'],
                  info_document=kafka_message, config=config['kafka'])


data_weather_sources = {
    "darksky": {
        "freq_rec": relativedelta(months=2),
        "measurement_type": "0",
        "source": DarkSky,
        "params": ["latitude", "longitude", "date_from", "date_to"],
    }
}


class WeatherMRJob(MRJob, ABC):
    def __read_config__(self):
        fn = glob.glob('*.pickle')
        self.config = pickle.load(open(fn[0], 'rb'))

    def mapper_init(self):
        self.__read_config__()

    def mapper(self, id_key, line):
        # map receives lat, lon and downloads DarkSky data,
        cp = {k: v for k, v in zip(["latitude", "longitude"], line.split("\t"))}
        mongo_logger.create(self.config['mongo_db'], self.config['datasources']['weather']['log'], 'gather',
                            log_exec=datetime.utcnow())
        sys.stderr.write(f"Processing: {'-'.join([str(cp['latitude']), str(cp['longitude'])])}\n")
        mongo_logger.log(f"Processing: {'-'.join([str(cp['latitude']), str(cp['longitude'])])}")
        weather_stations = \
            mongo_logger.get_connection()[self.config['datasources']['weather']['log_devices']]
        # get the highest page document log
        try:
            station = weather_stations.find(
                {"_id": f"{float(cp['latitude']):.3f}~{float(cp['longitude']):.3f}"}).sort([("page", -1)]).limit(1)[0]
        except IndexError:
            station = None
        if not station:
            # if there is no log document create a new one
            station = {
                "_id": f"{float(cp['latitude']):.3f}~{float(cp['longitude']):.3f}"
            }
        # create the data chunks we will gather the information for timeseries
        date_ini = datetime(2022, 1, 1)  # TODO refactor the date
        now = datetime.now() + timedelta(days=1)
        for t, type_params in data_weather_sources.items():
            sys.stderr.write(f"\tType: {t}\n")
            mongo_logger.log(f"\tType {t}")
            if t not in station:
                station[t] = {
                    "date_ini": date_ini,
                    "date_end": None
                }
                start_obtaining_date = date_ini
            elif station[t]['date_end']:
                start_obtaining_date = station[t]['date_end']
            else:
                start_obtaining_date = date_ini
            data = download_chunk(cp, type_params, self.config, start_obtaining_date, now)
            self.increment_counter('gathered', 'device', 1)
            if len(data) > 0:
                last_data = data.iloc[-1].ts.to_pydatetime().replace(tzinfo=None)
                save_weather_data(data, mongo_logger, self.config)
                station[t]['date_end'] = last_data
                sys.stderr.write(f"\t\t\tRequest sent\n")
                mongo_logger.log(f"\t\t\tRequest sent")

        sys.stderr.write(f"finished device\n")
        mongo_logger.log(f"finished device")
        weather_stations.replace_one({"_id": station['_id']}, station, upsert=True)
        self.increment_counter('gathered', 'device', 1)


if __name__ == '__main__':
    WeatherMRJob.run()
