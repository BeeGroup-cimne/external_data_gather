import argparse
import os
import settings
from GPG.GPG_gather import read_data_from_xlsx
from utils import save_to_kafka, save_to_hbase, read_config, mongo_logger

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("--user", "-u", help="The user importing the data", required=True)
    ap.add_argument("--namespace", "-n", help="The subjects namespace uri", required=True)
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. one of [k (kafka), h (hbase)]")
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-f", "GPG/data/Llistat immobles alta inventari (13-04-2021).xlsx",
                  "-n", "https://icaen.cat#", "-u", "icaen", "-s", "k"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()
    config = read_config(settings.conf_file)
    mongo_logger.create(config['mongo_db'], config['datasources']['GPG']['log'], 'gather', user=args.user)

    mongo_logger.log("start to parse a new file")
    try:
        gpg_list = read_data_from_xlsx(file=args.file)
        mongo_logger.log("file parsed correctly")
    except Exception as e:
        gpg_list = []
        mongo_logger.log("could not parse file: {e}")
        exit(1)

    if args.store == "k":
        chunks = range(0, len(gpg_list), settings.kafka_message_size)
        for num, i in enumerate(chunks):
            message_part = f"{num + 1}/{len(chunks)}"
            try:
                mongo_logger.log(f"sending {message_part} part")
                chunk = gpg_list[i:i + settings.kafka_message_size]
                kafka_message = {
                    "namespace": args.namespace,
                    "user": args.user,
                    "collection_type": "building",
                    "source": "gpg",
                    "row_keys": ["Num_Ens_Inventari"],
                    "logger": mongo_logger.export_log(),
                    "message_part": message_part,
                    "data": chunk
                }
                k_harmonize_topic = config["datasources"]["GPG"]["kafka_harmonize_topic"]
                k_store_topic = config["datasources"]["GPG"]["kafka_store_topic"]
                save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                mongo_logger.log(f"part {message_part}  sent correctly")
            except Exception as e:
                mongo_logger.log(f"error when sending part {message_part}: {e}")
    elif args.store == "h":
        mongo_logger.log(f"saving to hbase")
        try:
            h_table_name = f"{config['datasources']['GPG']['hbase_table']}_building_{args.user}"
            save_to_hbase(gpg_list, h_table_name, config['hbase_imported_data'], [("info", "all")],
                          row_fields=["Num_Ens_Inventari"])
            mongo_logger.log(f"successfully saved to hbase")
        except Exception as e:
            mongo_logger.log(f"error saving to hbase: {e}")
    else:
        mongo_logger.log(f"store {args.store} is not supported")
