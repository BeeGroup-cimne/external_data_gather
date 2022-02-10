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


def get_weather_stations(neo4j):
    driver = GraphDatabase.driver(**neo4j)
    with driver.session() as session:
        location = session.run(
            f"""
                Match (n:ns0__WeatherStation) 
                WHERE (n)-[:ns0__isObservedBy]-(:ns0__BuildingSpace) 
                RETURN n.ns1__lat as latitude, n.ns1__long as longitude
            """
        ).data()

        df_loc = pd.DataFrame.from_records(location)
        df_loc.latitude = df_loc.latitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.longitude = df_loc.longitude.apply(lambda x: f"{float(x):.3f}")
        df_loc.drop_duplicates(inplace=True)
    return df_loc


def get_timeseries_data(config):

    # generate config file
    mongo_logger.create(config['mongo_db'], config['datasources']['weather']['log'], 'gather',
                        log_exec=datetime.utcnow())
    job_config = config.copy()
    job_config.update({"kafka_message_size": settings.kafka_message_size, "mongo_logger": mongo_logger.export_log()})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get all CP to generate the MR input file
    stations = get_weather_stations(config['neo4j'])
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
        '--jobconf', f'mapreduce.job.maps=8'
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

    if os.getenv("PYCHARM_HOSTED"):
        args_t = []
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    config = read_config(settings.conf_file)
    get_timeseries_data(config)

