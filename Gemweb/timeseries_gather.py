from tempfile import NamedTemporaryFile

from pyhive import hive
import argparse
import pickle
import json
import os
import sys
sys.path.append(os.getcwd())
from utils import *
from Gemweb.gemweb_gather_mr import Gemweb_gather
# read config file and send it to mapreduce
with open("./config.json") as config_f:
    config = json.load(config_f)

mongo = connection_mongo(config['mongo_db'])
data_source = config['datasources']['gemweb']
for connection in mongo[data_source['info']].find({}):
    # connection = mongo[data_source['info']].find_one({})
    job_config = dict()
    job_config['connection'] = connection.copy()
    job_config['config'] = dict()
    job_config['config']['data_source'] = data_source
    job_config['config']['mongo_connection'] = config['mongo_db']
    job_config['config']['hbase_connection'] = config['hbase']

    f = NamedTemporaryFile(delete=False, suffix='.json')
    f.write(pickle.dumps(job_config))
    f.close()

    # create supplies hdfs file to perform mapreduce
    create_table_hbase = """CREATE EXTERNAL TABLE supplies_aux(id string, value string) 
                            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
                            WITH SERDEPROPERTIES (
                                'hbase.table.name' = 'raw_data:gemweb_supplies_icaen', 
                                "hbase.columns.mapping" = ":key,info:cups"
                            )"""

    save_id_to_file = """INSERT OVERWRITE DIRECTORY '/tmp/supplies_aux/' SELECT id FROM supplies_aux limit 20"""

    cursor = hive.Connection("master1.internal", 10000, database="gemweb").cursor()

    cursor.execute(create_table_hbase)
    cursor.execute(save_id_to_file)

    MOUNTS='YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE='YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=beerepo.tech.beegroup-cimne.com:5000/python3-mr'
    RUNTYPE='YARN_CONTAINER_RUNTIME_TYPE=docker'
    mr_job = Gemweb_gather(args=[
        '-r', 'hadoop', 'hdfs://{}'.format("/tmp/supplies_aux/"), '--file', f.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', 'mapreduce.map.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),
        '--jobconf', 'mapreduce.reduce.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),
        '--jobconf', 'mapreduce.job.name=gemweb_import',
        '--jobconf', 'mapreduce.job.reduces=10'
    ])

    with mr_job.make_runner() as runner:
        runner.run()
