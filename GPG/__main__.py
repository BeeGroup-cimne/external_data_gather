import argparse
import json
import os
import uuid

from GPG.GPG_gather import read_data_from_xlsx, harmonize_organizations
from utils import save_to_kafka

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("-u", "--user", required=True, help="Excel file path to parse")
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-f", "GPG/data/Llistat immobles alta inventari (13-04-2021).xlsx", "-u", "icaen"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    with open("./config.json") as config_f:
        config = json.load(config_f)

    gpg_list = read_data_from_xlsx(file=args.file)
    harmonize_organizations(gpg_list, "icaen", config['neo4j'])
    kafka_messages = {
        "organization_name": "Generalitat de Catalunya",
        "namespace": "http://icaen.cat#",
        "user": args.user,
        "collection_type": "building",
        "source": "gpg",
        "row_keys": ["Num_Ens_Inventari"],
        "data": gpg_list
    }
    save_to_kafka(topic="raw-hbase", info_document=kafka_messages, config=config['kafka'])
    save_to_kafka(topic="harmonize-raw-data", info_document=kafka_messages, config=config['kafka'])

