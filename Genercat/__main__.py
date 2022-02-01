import argparse
import hashlib
import os
import sys
from datetime import datetime

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
    mongo_logger.create(config['mongo_db'], config['datasources']['genercat']['log'], 'gather', user=args.user,
                        log_exec=datetime.utcnow())
    mongo_logger.log("start to parse a new file")
    sys.stderr.write("start to parse a new file\n")
    try:
        data = get_data(args.file)
        file_id = hashlib.md5(bytes(args.file.split("/")[-1], encoding="utf-8")).hexdigest()
        for i, datat in enumerate(data):
            datat['id_'] = f"{file_id}~{i}"
        mongo_logger.log("file parsed with success")
        sys.stderr.write("file parsed with success\n")

    except Exception as e:
        data = []
        mongo_logger.log(f"error parsing the file: {e}")
        sys.stderr.write(f"error parsing the file: {e}\n")
        exit(1)
    if args.store == "k":
        chunks = range(0, len(data), settings.kafka_message_size)
        for num, i in enumerate(chunks):
            message_part = f"{num + 1}/{len(chunks)}"
            try:
                mongo_logger.log(f"sending {message_part} part")
                sys.stderr.write(f"sending {message_part} part\n")
                chunk = data[i:i + settings.kafka_message_size]
                kafka_message = {
                    "namespace": args.namespace,
                    "user": args.user,
                    "collection_type": "eem",
                    "source": "genercat",
                    "row_keys": ["id_"],
                    "logger": mongo_logger.export_log(),
                    "message_part": message_part,
                    "data": chunk
                }
                k_harmonize_topic = config["datasources"]["genercat"]["kafka_harmonize_topic"]
                k_store_topic = config["datasources"]["genercat"]["kafka_store_topic"]
                save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                mongo_logger.log(f"part {message_part} sent correctly")
                sys.stderr.write(f"part {message_part} sent correctly\n")
            except Exception as e:
                mongo_logger.log(f"error when sending part {message_part}: {e}")
                sys.stderr.write(f"error when sending part {message_part}: {e}\n")
    elif args.store == "h":
        mongo_logger.log(f"saving to hbase")
        sys.stderr.write(f"saving to hbase\n")
        try:
            h_table_name = f"{config['datasources']['genercat']['hbase_table']}_eem_{args.user}"
            save_to_hbase(data, h_table_name, config['hbase_imported_data'], [("info", "all")], row_fields=["id_"])
            mongo_logger.log(f"successfully saved to hbase")
            sys.stderr.write(f"successfully saved to hbase\n")
        except Exception as e:
            mongo_logger.log(f"error saving to hbase: {e}")
            sys.stderr.write(f"error saving to hbase: {e}\n")
    else:
        mongo_logger.log(f"store {args.store} is not supported")
        sys.stderr.write(f"store {args.store} is not supported\n")

