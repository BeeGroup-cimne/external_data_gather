import os
import pickle
from tempfile import NamedTemporaryFile

from beedis import datadis, ENDPOINTS

import settings
from Datadis.datadis_utils import get_users, generate_input_tsv
from Datadis.datadis_gather_mr import DatadisMRJob
from utils import decrypt, put_file_to_hdfs, remove_file, remove_file_from_hdfs, mongo_logger, \
    save_to_kafka, save_to_hbase


def get_static_data(store, config):
    for user in get_users(config['neo4j']):
        mongo_logger.create(config['mongo_db'], config['datasources']['datadis']['log'], 'gather',
                            user=user['user'], datasource_user=user['username'])
        mongo_logger.log("Gathering datadis supplies")
        try:
            mongo_logger.log("Log in datadis API")
            password = decrypt(eval(user['password']), os.getenv(config['encript_pass']['environment']))
            datadis.connection(user['username'], password, timezone="UTC", timeout=100)
            mongo_logger.log("Obtaining datadis supplies")
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
            for sup in supplies:
                sup['nif'] = user['username']
            mongo_logger.log(f"Success gathering datadis supplies")
        except Exception as e:
            mongo_logger.log(f"Error gathering datadis supplies: {e}")
            continue
        if store == "k":
            mongo_logger.log(f"sending data to kafka store topic")
            k_store_topic = config["datasources"]["datadis"]["kafka_store_topic"]
            k_harmonize_topic = config["datasources"]["datadis"]["kafka_harmonize_topic"]
            chunks = range(0, len(supplies), settings.kafka_message_size)
            mongo_logger.log(f"sending supplies data to store and harmonizer topics")
            for num, i in enumerate(chunks):
                message_part = f"{num + 1}/{len(chunks)}"
                try:
                    mongo_logger.log(f"sending {message_part} part")
                    chunk = supplies[i:i + settings.kafka_message_size]
                    kafka_message = {
                        "namespace": user["namespace"],
                        "user": user["user"],
                        "collection_type": "supplies",
                        "source": "datadis",
                        "row_keys": ["cups"],
                        "logger": mongo_logger.export_log(),
                        "message_part": message_part,
                        "data": chunk
                    }
                    save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                    save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                    mongo_logger.log(f"part {message_part}  sent correctly")
                except Exception as e:
                    mongo_logger.log(f"error when sending part {message_part}: {e}")
        elif store == "h":
            mongo_logger.log(f"Saving supplies to HBASE")
            try:
                h_table_name = f"{config['datasources']['datadis']['hbase_table']}_suppiles_{user['user']}"
                save_to_hbase(supplies, h_table_name, config['hbase_imported_data'], [("info", "all")],
                              row_fields=["cups"])
                mongo_logger.log(f"Supplies saved successfully")
            except Exception as e:
                mongo_logger.log(f"Error saving datadis supplies to HBASE: {e}")
        else:
            mongo_logger.log(f"store {store} is not supported")


def get_timeseries_data(store, config):

    # generate config file
    job_config = config.copy()
    job_config.update({"store": store, "kafka_message_size": settings.kafka_message_size})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get Users to generate the MR input file
    users = get_users(config)
    local_input = generate_input_tsv(config, users)
    input_mr = put_file_to_hdfs(source_file_path=local_input, destination_file_path='/tmp/datadis_tmp/')
    remove_file(local_input)

    # Map Reduce
    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-datadis'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    datadis_job = DatadisMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(input_mr),
        '--file', config_file.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f"mapreduce.job.name=datadis_import",
        '--jobconf', f'mapreduce.job.reduces=2'
    ])
    try:
        with datadis_job.make_runner() as runner:
            runner.run()
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)
    except Exception as e:
        print(f"error in map_reduce: {e}")
        remove_file_from_hdfs(input_mr)
        remove_file(config_file.name)
