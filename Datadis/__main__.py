import argparse
import os
import settings
from Datadis.datadis_gather import get_timeseries_data
from utils import read_config

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Gathering data from Datadis')
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. One of [k (kafka), h (hbase)]")
    ap.add_argument("-p", "--policy", required=True, help="The policy for updating data. One of [last, repair]")

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["--store", "k", "-p", "last"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    config = read_config(settings.conf_file)
    get_timeseries_data(args.store, args.policy, config)
