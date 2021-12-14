import json
import pickle
from tempfile import NamedTemporaryFile

from pyhive import hive

from datadis_gather_mr import DatadisMRJob
import argparse


def get_config(path):
    f = open(path, "r")
    return json.load(f)


if __name__ == '__main__':
    # Arguments

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data_type", default="hour", type=str)
    args = vars(parser.parse_args())

    # Read Config
    config = get_config('config.json')

    # New Config
    job_config = config.copy()

    for i in args:
        job_config.update({i: args[i]})

    f = NamedTemporaryFile(delete=False, suffix='.pickle')
    f.write(pickle.dumps(job_config))
    f.close()

    # Set Table and File name
    hbase_table = "raw_data:datadis_supplies_icaen"
    hdfs_file = "datadis_supplies_icaen"

    # Query to create table
    create_table_hbase = f"""CREATE EXTERNAL TABLE {hdfs_file} (id string, address string, postalCode string, province string, municipality string, distributor string, validDateFrom string, validDateTo string, pointType string, distributorCode string)
                            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                            WITH SERDEPROPERTIES (
                                'hbase.table.name' = '{hbase_table}', "hbase.columns.mapping" = ":key,info:address,info:postalCode,info:province,info:municipality,info:distributor,info:validDateFrom,info:validDateTo,info:pointType,info:distributorCode")"""

    # Query to insert data to the table
    save_id_to_file = f"""INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * FROM {hdfs_file}"""

    # Query to remove temporal table
    remove_hbase_table = f"""DROP TABLE {hdfs_file}"""

    # Init Hive connection
    cursor = hive.Connection("master1.internal", 10000, database='bigg').cursor()

    # Execute queries
    cursor.execute(create_table_hbase)
    cursor.execute(save_id_to_file)
    cursor.execute(remove_hbase_table)
    cursor.close()

    # Map Reduce

    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-datadis'
    RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

    datadis_job = DatadisMRJob(args=[
        '-r', 'hadoop', 'hdfs://{}'.format(f"/tmp/{hdfs_file}/"),
        '--file', f.name,
        '--file', 'utils.py#utils.py',
        '--jobconf', f'mapreduce.map.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.reduce.env={MOUNTS},{IMAGE},{RUNTYPE}',
        '--jobconf', f'mapreduce.job.name=datadis_import',
        '--jobconf', f'mapreduce.job.reduces=1',
        '--output-dir', 'datadis_output'
    ])

    with datadis_job.make_runner() as runner:
        runner.run()
