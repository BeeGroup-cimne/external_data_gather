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
    # create supplies hdfs file to perform mapreduce
    hbase_table = f"raw_data:gemweb_supplies_{connection['user']}"
    hdfs_file = f"supplies_{connection['user']}"

    create_table_hbase = f"""CREATE EXTERNAL TABLE {hdfs_file}(id string, value string)
                            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                            WITH SERDEPROPERTIES (
                                'hbase.table.name' = '{hbase_table}',
                                "hbase.columns.mapping" = ":key,info:cups"
                            )"""

    #save_id_to_file = f"""INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' SELECT id FROM {hdfs_file}"""
    save_id_to_file = f"""
    INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' select id from supplies_icaen where id in ( 95109, 95112, 95113, 95123, 95124, 95134, 95135, 95145, 95146, 95147, 95150, 95156, 95157, 95158, 95160, 95161, 95167, 95168, 95169, 95170, 95171, 95172, 95178, 95179, 95180, 95181, 95182, 95183, 95185, 95187, 95188, 95189, 95190, 95191, 95192, 95193, 95194, 95195, 95196, 95198, 95199, 95200, 95201, 95202, 95203, 95204, 95206, 95207, 95208, 95209, 95210, 95211, 95212, 95213, 95214, 95215, 95217, 95218, 95219, 95220, 95221, 95222, 95223, 95224, 95225, 95226, 95227, 95228, 95229, 95230, 95231, 95232, 95233, 95234, 95235, 95236, 95237, 95238, 95239, 95240, 95241, 95242, 95243, 95244, 95245, 95246, 95247, 95248, 95249, 95250, 95251, 95252, 95253, 95254, 95255, 95256, 95257, 95258, 95259, 95260, 95261, 95262, 95263, 95264, 95265, 95266, 95267, 95268, 95269, 95270, 95271, 95272, 95273, 95274, 95275, 95276, 95277, 95278, 95279, 95280, 95281, 95282, 95283, 95284, 95285, 95286, 95287, 95288, 95289, 95291, 95293, 95295, 95296, 95297, 95298, 95299, 95300, 95301, 95302, 95303, 95304, 95305, 95306, 95307, 95308, 95309, 95310, 95311, 95312, 95313, 95314, 95315, 95316, 95317, 95318, 95367, 95369, 95370, 95371, 95372, 95373, 95374, 95375, 95376, 95377, 95378, 95379, 95380, 95381, 95382, 95383, 95384, 95385, 95386, 95387, 95388, 95389, 95390, 95391, 95392, 95393, 95394, 95395, 95396, 95397, 95398, 95399, 95400, 95401, 95402, 95403, 95404, 95405, 95439, 95440, 95441, 95442, 95443, 95444, 95445, 95446, 95447, 95448, 95449, 95450, 95452, 95453, 95528, 95529, 95557, 95558, 95567, 95569, 95580, 95613, 95623, 95624, 95705, 95774, 95775, 95776, 95777, 95778, 95779, 95780, 95781, 95782, 95783, 95784, 95785, 95786, 95787, 95788, 95789, 95790, 95791, 95792, 95793, 95794, 95795, 95796, 95797, 95798, 95799, 95800, 95801, 95802, 95803, 95804, 96029, 96385, 96959, 96996, 96997, 96998, 96999, 97000, 97003)
    """
    remove_hbase_table = f"""DROP TABLE {hdfs_file}"""
    cursor = hive.Connection("master1.internal", 10000, database="gemweb").cursor()
    cursor.execute(create_table_hbase)
    cursor.execute(save_id_to_file)
    cursor.execute(remove_hbase_table)
    cursor.close()

    job_config = dict()
    job_config['connection'] = connection.copy()
    job_config['config'] = dict()
    job_config['config']['data_source'] = data_source
    job_config['config']['mongo_connection'] = config['mongo_db']
    job_config['config']['hbase_connection'] = config['hbase']

    f = NamedTemporaryFile(delete=False, suffix='.json')
    f.write(pickle.dumps(job_config))
    f.close()

    MOUNTS ='YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE ='YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=beerepo.tech.beegroup-cimne.com:5000/python3-mr'
    RUNTYPE ='YARN_CONTAINER_RUNTIME_TYPE=docker'
    mr_job = Gemweb_gather(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(f"/tmp/{hdfs_file}/"), '--file', f.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', 'mapreduce.map.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),
        '--jobconf', 'mapreduce.reduce.env={},{},{}'.format(MOUNTS, IMAGE, RUNTYPE),
        '--jobconf', 'mapreduce.job.name=gemweb_import',
        '--jobconf', 'mapreduce.job.reduces=8',
    ])

    with mr_job.make_runner() as runner:
        runner.run()
