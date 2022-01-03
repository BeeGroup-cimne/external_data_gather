import argparse
import ast
import os
import pickle
from tempfile import NamedTemporaryFile

from neo4j import GraphDatabase

from datadis_gather_mr import DatadisMRJob
from utils import decrypt, put_file_to_hdfs, remove_file, remove_file_from_hdfs, get_json_config


# sys.path.append(os.getcwd())


def get_users(config):
    driver = GraphDatabase.driver(config['neo4j']['uri'], auth=(config['neo4j']['user'], config['neo4j']['password']))
    with driver.session() as session:
        users = session.run(
            f"""
                Match (n:DatadisSource)<-[:ns0__hasSource]->(o:ns0__Organization)
                CALL{{
                    With o
                    Match (o)<-[*]-(d:ns0__Organization)
                    WHERE NOT (d)<-[:ns0__hasSubOrganization]-() return d}}
                    return n.username, n.password, d.user_id
            """).data()

    return users


def generate_tsv(config, data):
    with NamedTemporaryFile(delete=False, suffix=".tsv", mode='w') as file:
        for i in data:
            password = config['key_decoder']
            enc_dict = ast.literal_eval(i['n.password'])
            password_decoded = decrypt(enc_dict, password).decode('utf-8')
            tsv_str = f"{i['n.username']}\t{password_decoded}\t{i['d.user_id']}\n"
            file.write(tsv_str)
    return file.name


if __name__ == '__main__':

    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data_type", required=True, type=str)

    if os.getenv("PYCHARM_HOSTED"):
        args = vars(parser.parse_args(['-d', "hourly_consumption"]))
    else:
        args = vars(parser.parse_args())

    # Read Config
    config = get_json_config('config.json')

    # New Config
    job_config = config.copy()
    job_config.update(args)

    f = NamedTemporaryFile(delete=False, prefix='config_job_', suffix='.pickle')
    f.write(pickle.dumps(job_config))
    f.close()

    # Get Users
    users = get_users(config)

    # Create and Save TSV File
    tsv_file_path = generate_tsv(config, users[:2])

    input_mr_file_path = put_file_to_hdfs(source_file_path=tsv_file_path, destination_file_path='/tmp/datadis_tmp/')
    remove_file(tsv_file_path)

    # Map Reduce

    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-datadis'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    datadis_job = DatadisMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(input_mr_file_path),
        '--file', f.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.job.name=datadis_import',
        '--jobconf', f'mapreduce.job.reduces=2'
    ])

    with datadis_job.make_runner() as runner:
        runner.run()

    remove_file_from_hdfs(input_mr_file_path)
    remove_file(f.name)
