import settings
import argparse
import json
import os

from GPG.GPG_gather import read_data_from_xlsx
from utils import save_to_kafka, save_to_hbase

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--organization_name", "-name", help="The main organization name", required=True)
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. one of [k (kafka), h (hbase)]")
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-f", "GPG/data/Llistat immobles alta inventari (13-04-2021).xlsx",
                  "-name", "Generalitat de Catalunya", "-n", "http://icaen.cat#","-u", "icaen", "-s", "k"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    with open(settings.conf_file) as config_f:
        config = json.load(config_f)

    gpg_list = read_data_from_xlsx(file=args.file)

    if args.store == "k":
        for i in range(0, len(gpg_list), settings.kafka_message_size):
            chunk = gpg_list[i:i+settings.kafka_message_size]
            kafka_message = {
                "organization_name": args.organization_name,
                "namespace": args.namespace,
                "user": args.user,
                "collection_type": ["building"],
                "source": "gpg",
                "row_keys": [["Num_Ens_Inventari"]],
                "data": [chunk]
            }
            k_topic = config["datasources"]["GPG"]["kafka_topic"]
            save_to_kafka(topic=k_topic, info_document=kafka_message, config=config['kafka'])
    elif args.store == "h":
        h_table_name = f"{config['datasources']['GPG']['hbase_table']}_building_{args.user}"
        save_to_hbase(gpg_list, h_table_name, config['hbase_imported_data'], [("info", "all")], row_fields=["Num_Ens_Inventari"])