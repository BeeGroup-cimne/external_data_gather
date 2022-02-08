import argparse
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
from neo4j import GraphDatabase

from utils import read_config, mongo_connection
import os
import pickle
from tempfile import NamedTemporaryFile
import settings
from utils import decrypt, put_file_to_hdfs, remove_file, remove_file_from_hdfs, mongo_logger, \
    save_to_kafka, save_to_hbase
from weather.weather_gather_mr import WeatherMRJob


def generate_input_tsv(df):
    with NamedTemporaryFile(delete=False, suffix=".tsv", mode='w') as file:
        for _, item in df.iterrows():
            line = '\t'.join([str(v) for _, v in item.items()]) + "\n"
            file.write(line)
        return file.name


def get_weather_stations(neo4j, cp_file):
    driver = GraphDatabase.driver(**neo4j)
    with driver.session() as session:
        location = session.run(
            f"""
                Match(n:ns0__Building)-[:ns0__hasLocationInfo]-(l:ns0__LocationInfo)
                WHERE l.ns0__addressLatitude IS NOT NULL and l.ns0__addressLongitude IS NOT NULL
                RETURN toFloat(l.ns0__addressLatitude) AS latitude, toFloat(l.ns0__addressLongitude) AS longitude
            """
        ).data()
        postal_code = session.run(
            f"""
                Match(n:ns0__Building)-[:ns0__hasLocationInfo]-(l:ns0__LocationInfo) 
                WHERE l.ns0__addressPostalCode IS NOT NULL and (l.ns0__addressLatitude IS NULL or l.ns0__addressLongitude IS NULL)
                RETURN DISTINCT l.ns0__addressPostalCode as postal_code
            """
        ).data()
        with open(cp_file) as f:
            cpcat = json.load(f)
        for cp in postal_code:
            try:
                lon, lat = cpcat[cp['postal_code']]

            except:
                try:
                    lat, lon = cpcat[str(int(cp['postal_code'])+1).zfill(5)]
                except:
                    print(f"postal code {cp['postal_code']} does not exist")
                    continue
            location.append({"latitude": lat, "longitude": lon})
        df_loc = pd.DataFrame.from_records(location)
        df_loc.latitude = df_loc.latitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.longitude = df_loc.longitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.drop_duplicates(inplace=True)
    return df_loc


def get_timeseries_data(config, cp_file):

    # generate config file
    job_config = config.copy()
    job_config.update({"kafka_message_size": settings.kafka_message_size})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get all CP to generate the MR input file
    stations = get_weather_stations(config['neo4j'], cp_file)
    local_input = generate_input_tsv(stations.iloc[:10])  # TODO: remove this limit
    input_mr = put_file_to_hdfs(source_file_path=local_input, destination_file_path='/tmp/weather_tmp/')
    remove_file(local_input)

    # Map Reduce
    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-weather'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    weather_job = WeatherMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(input_mr),
        '--file', config_file.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f"mapreduce.job.name=weather_import",
        '--jobconf', f'mapreduce.job.reduces=8'
    ])
    try:
        with weather_job.make_runner() as runner:
            runner.run()
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)
    except Exception as e:
        print(f"error in map_reduce: {e}")
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Gathering weather data from CAMS, DARKSKY and METEOGALICIA')
    ap.add_argument("-f", "--file", required=True, help="The file containing postal codes and locations")

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["--store", "k", "-p", "last"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    config = read_config(settings.conf_file)
    get_timeseries_data(config, args.file)

