import argparse
import os
from beedis import datadis, ENDPOINTS
import settings
from Datadis.datadis_utils import get_users
from utils import read_config, mongo_logger, save_to_hbase, save_to_kafka, decrypt

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. one of [k (kafka), h (hbase)]")
    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-s", "k"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()
    config = read_config(settings.conf_file)
    for user in get_users(config['neo4j']):
        mongo_logger.create(config['mongo_db'], config['datasources']['datadis']['log'], 'gather',
                            user['user'])
        mongo_logger.log("Gathering datadis supplies")
        try:
            mongo_logger.log("Log in datadis API")
            password = decrypt(eval(user['password']), os.getenv(config['encript_pass']['environment']))
            datadis.connection(user['username'], password, timezone="UTC", timeout=100)
            mongo_logger.log("Obtaining datadis supplies")
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
            for sup in supplies:
                sup['nif'] = user['username']
            mongo_logger.log(f"Success gathering datadis supplies")
        except Exception as e:
            mongo_logger.log(f"Error gathering datadis supplies: {e}")
            continue
        if args.store == "k":
            mongo_logger.log(f"sending data to kafka store topic")
            k_store_topic = config["datasources"]["datadis"]["kafka_store_topic"]
            k_harmonize_topic = config["datasources"]["datadis"]["kafka_harmonize_topic"]
            chunks = range(0, len(supplies), settings.kafka_message_size)
            mongo_logger.log(f"sending supplies data to store and harmonizer topics")
            for num, i in enumerate(chunks):
                message_part = f"{num + 1}/{len(chunks)}"
                try:
                    mongo_logger.log(f"sending {message_part} part")
                    chunk = supplies[i:i + settings.kafka_message_size]
                    kafka_message = {
                        "namespace": user["namespace"],
                        "user": user["user"],
                        "collection_type": "supplies",
                        "source": "datadis",
                        "row_keys": ["cups"],
                        "logger": mongo_logger.export_log(),
                        "message_part": message_part,
                        "data": chunk
                    }
                    save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                    save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                    mongo_logger.log(f"part {message_part}  sent correctly")
                except Exception as e:
                    mongo_logger.log(f"error when sending part {message_part}: {e}")
        elif args.store == "h":
            mongo_logger.log(f"Saving supplies to HBASE")
            try:
                h_table_name = f"{config['datasources']['datadis']['hbase_table']}_suppiles_{user['user']}"
                save_to_hbase(supplies, h_table_name, config['hbase_imported_data'], [("info", "all")],
                              row_fields=["cups"])
                mongo_logger.log(f"Supplies saved successfully")
            except Exception as e:
                mongo_logger.log(f"Error saving datadis supplies to HBASE: {e}")
        else:
            mongo_logger.log(f"store {args.store} is not supported")

            # TODO: contracts

