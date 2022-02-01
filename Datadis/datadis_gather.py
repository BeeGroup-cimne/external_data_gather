import os
import pickle
from tempfile import NamedTemporaryFile

from beedis import datadis, ENDPOINTS

import settings
from Datadis.datadis_utils import get_users, generate_input_tsv
from Datadis.datadis_gather_mr import DatadisMRJob
from utils import decrypt, put_file_to_hdfs, remove_file, remove_file_from_hdfs, mongo_logger, \
    save_to_kafka, save_to_hbase


def get_timeseries_data(store, policy, config):

    # generate config file
    job_config = config.copy()
    job_config.update({"store": store, "kafka_message_size": settings.kafka_message_size, "policy": policy})
    config_file = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    config_file.write(pickle.dumps(job_config))
    config_file.close()

    # Get Users to generate the MR input file
    users = get_users(config['neo4j'])
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
        '--jobconf', f'mapreduce.job.reduces=8'
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
