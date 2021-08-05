import json
from logger import setup_logger
import subprocess

from pymongo import MongoClient


def read_configuration(path='config.json'):
    try:
        with open(path) as config_file:
            return json.load(config_file)
    except Exception as ex:
        log.error(ex)


def generate_mongo_uri(config):
    try:
        if config['password'] and config['user']:
            return 'mongodb://%s:%s@%s:%s/' % (
                config['user'], config['password'], config['host'], config['port'])
        else:
            return 'mongodb://%s:%s/' % (config['host'], config['port'])
    except Exception as ex:
        log.error(ex)


def generate_tsv(collection, output_path='out/output.tsv'):
    try:
        with open(output_path, 'w') as file:
            # file.write('Email\tPassword\tAPI_KEY\n')  # Header
            for i in collection.find({}):
                file.write('%s\t%s\t%s\n' % (i['email'], i['password'], i['api_application']))
    except Exception as ex:
        log.error(ex)


def put_file_to_hdfs(source_file_path='out/output.tsv', destination_file_path='/'):
    try:
        output = subprocess.call("hdfs dfs -put -f %s %s" % (source_file_path, destination_file_path), shell=True)
    except Exception as ex:
        log.error(ex)


if __name__ == '__main__':
    log = setup_logger('manage_credentials')

    config = read_configuration()
    log.info("Configuration has been read successfully.")

    mongo_uri = generate_mongo_uri(config['mongo_db'])
    log.info("MongoDB URI has been generated successfully.")

    mongo_connexion = MongoClient(mongo_uri)
    db = mongo_connexion[config['mongo_db']['db']]
    collection = db['ixon_users']
    log.info("MongoDB connexion has been connected successfully.")

    generate_tsv(collection)
    log.info("TSV File has been created successfully.")

    put_file_to_hdfs()
    log.info("TSV File has been uploaded to HDFS successfully.")

    exit(0)
