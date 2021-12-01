import json
import os
import subprocess
import tempfile
from pymongo import MongoClient

from ixon_mrjob import MRIxonJob
from logger import setup_logger


def read_configuration(path='config.json'):
    try:
        with open(path) as config_file:
            log.info("Configuration has been read successfully.")
            return json.load(config_file)
    except Exception as ex:
        log.error(ex)


def generate_mongo_uri(config):
    try:
        if config['password'] and config['user']:
            return 'mongodb://%s:%s@%s:%s/%s' % (
                config['user'], config['password'], config['host'], config['port'], config['db'])
        else:
            return 'mongodb://%s:%s/%s' % (config['host'], config['port'], config['db'])
    except Exception as ex:
        log.error(ex)


def generate_tsv(collection):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tsv", mode='w') as file:
            # file.write('Email\tPassword\tAPI_KEY\n')  # Header
            for i in collection.find({}):
                file.write('%s\t%s\t%s\t%s\n' % (i['email'], i['password'], i['api_application'], i['description']))
        return file.name
    except Exception as ex:
        log.error(ex)


def put_file_to_hdfs(source_file_path, destination_file_path='/tmp/ixon_tmp/'):
    try:
        output = subprocess.call(f"hdfs dfs -put -f {source_file_path} {destination_file_path}", shell=True)
        os.remove(source_file_path)
        return '/tmp/ixon_tmp/' + source_file_path.split('/')[-1]
    except Exception as ex:
        log.error(ex)


if __name__ == '__main__':
    log = setup_logger('manage_credentials')

    config = read_configuration()

    mongo_uri = generate_mongo_uri(config['mongo_db'])

    try:
        mongo_connexion = MongoClient(mongo_uri)
        db = mongo_connexion[config['mongo_db']['db']]
        collection = db['ixon_users']
        log.info("MongoDB connexion has been connected successfully.")

        tmp_path = generate_tsv(collection)
        log.info("TSV File has been created successfully.")

        hdfs_out_path = put_file_to_hdfs(source_file_path=tmp_path)
        log.info("TSV File has been uploaded to HDFS successfully.")
    except Exception as ex:
        log.error(ex)

    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/dev/net/tun:/dev/net/tun:rw'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/admin/ixon_mr'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    mr_job = MRIxonJob(args=[
        '-r', 'hadoop', 'hdfs://%s' % hdfs_out_path,
        '--file', 'Ixon.py',
        '--file', 'utils.py#utils.py',
        '--file', 'vpn_files/vpn_template_0.ovpn',
        '--file', 'vpn_files/vpn_template_1.ovpn',
        '--file', 'vpn_files/vpn_template_2.ovpn',
        '--file', 'vpn_files/vpn_template_3.ovpn',
        '--file', 'vpn_files/vpn_template_4.ovpn',
        '--file', 'config.json#config.json',
        '--jobconf', 'mapreduce.map.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),  # PRIVILEGED, DISABLE),
        '--jobconf', 'mapreduce.reduce.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),  # PRIVILEGED, DISABLE),
        '--jobconf', 'mapreduce.job.name=ixon_gather',
        '--jobconf', 'mapreduce.job.reduces=5',
        # '--output-dir', 'ixon_output'
    ])

    with mr_job.make_runner() as runner:
        runner.run()

    exit(0)
