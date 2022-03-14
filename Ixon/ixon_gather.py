import tempfile

from ixon_mrjob import MRIxonJob
from utils import connection_mongo, get_json_config, put_file_to_hdfs, remove_file_from_hdfs, remove_file


def generate_tsv(collection):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tsv", mode='w') as file:
            for i in collection.find({}):
                file.write(f"{i['email']}\t{i['password']}\t{i['api_application']}\t{i['description']}\n")
        return file.name
    except Exception as ex:
        print(str(ex))


if __name__ == '__main__':
    # Read Config
    config = get_json_config('config.json')

    # Connect to MongoDB
    db = connection_mongo(config['mongo_db'])
    collection = db['ixon_users']

    # Generate TSV File
    tmp_path = generate_tsv(collection)

    # Load TSV to hdfs
    hdfs_out_path = put_file_to_hdfs(source_file_path=tmp_path, destination_file_path="/tmp/ixon_tmp/")

    # MapReduce Config

    MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/dev/net/tun:/dev/net/tun:rw'
    IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/mr/mr-ixon:latest'
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
        '--jobconf', 'mapreduce.job.reduces=5'
    ])

    with mr_job.make_runner() as runner:
        runner.run()

    # Remove generated files
    remove_file(tmp_path)
    remove_file_from_hdfs(hdfs_out_path)
