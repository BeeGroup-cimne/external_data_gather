import argparse
import os
import uuid

import settings
from Genercat.genercat_gather import get_data
from utils import save_to_kafka, save_to_hbase, read_config, mongo_logger

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. one of [k (kafka), h (hbase)]")
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-f", "Genercat/data/genercat.xls",
                  "-n", "https://icaen.cat#", "-u", "icaen", "-s", "k"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()
    config = read_config(settings.conf_file)
    mongo_logger.create(config['mongo_db'], config['datasources']['genercat']['log'], 'gather', user=args.user)
    mongo_logger.log("start to parse a new file")
    try:
        data = get_data(args.file)
        mongo_logger.log("file parsed with success")
    except Exception as e:
        data = []
        mongo_logger.log(f"error parsing the file: {e}")
        exit(1)
    if args.store == "k":
        file_id = uuid.uuid4()
        for i, datat in enumerate(data):
            datat['id_'] = f"{file_id}~{i}"
        chunks = range(0, len(data), settings.kafka_message_size)
        for num, i in enumerate(chunks):
            message_part = f"{num + 1}/{len(chunks)}"
            try:
                mongo_logger.log(f"sending {message_part} part")
                chunk = data[i:i + settings.kafka_message_size]
                kafka_message = {
                    "namespace": args.namespace,
                    "user": args.user,
                    "collection_type": "eem",
                    "source": "genercat",
                    "row_keys": None,
                    "logger": mongo_logger.export_log(),
                    "message_part": message_part,
                    "data": chunk
                }
                k_harmonize_topic = config["datasources"]["genercat"]["kafka_harmonize_topic"]
                k_store_topic = config["datasources"]["genercat"]["kafka_store_topic"]
                save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                mongo_logger.log(f"part {message_part} sent correctly")
            except Exception as e:
                mongo_logger.log(f"error when sending part {message_part}: {e}")
    elif args.store == "h":
        mongo_logger.log(f"saving to hbase")
        try:
            h_table_name = f"{config['datasources']['genercat']['hbase_table']}_eem_{args.user}"
            save_to_hbase(data, h_table_name, config['hbase_imported_data'], [("info", "all")])
            mongo_logger.log(f"successfully saved to hbase")
        except Exception as e:
            mongo_logger.log(f"error saving to hbase: {e}")
    else:
        mongo_logger.log(f"store {args.store} is not supported")
