import argparse

import gemweb
import json
from datetime import datetime
import os
import sys

import pandas as pd

import settings
from Gemweb.gemweb_gather import get_data
from utils import read_config, mongo_logger, save_to_hbase, save_to_kafka

data_types = {
    "entities": {
        "name": "entities",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "entitats"
    },
    "buildings": {
        "name": "buildings",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "centres_consum"
    },
    "supplies": {
        "name": "supplies",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "subministraments"
    },
    "solarpv": {
        "name": "solarpv",
        "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
        "category": "instalacions_solars"
    },
    # "invoices": {
    #     "name": "invoices",
    #     "endpoint": gemweb.ENDPOINTS.GET_INVENTORY,
    #     "category": "factures"
    # }
}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-s", "--store", required=True, help="Where to store the data. one of [k (kafka), h (hbase)]")

    if os.getenv("PYCHARM_HOSTED"):
        args_t = ["-s", "k"]
        args = ap.parse_args(args_t)
    else:
        args = ap.parse_args()

    config = read_config(settings.conf_file)

    # Connection to neo4j to get all GemwebSources user,passwords and main org
    # TODO: Query Neo4j to get connection data
    gemweb_connections = [{"username": "", "password": "", "namespace": "http://icaen.cat#",
                           "user": "icaen"}]
    for connection in gemweb_connections:
        mongo_logger.create(config['mongo_db'], config['datasources']['gemweb']['log'], 'gather',
                            connection['user'])
        try:
            mongo_logger.log("log in to gemweb API")
            # TODO: LOGIN TO GEMWEB
            gemweb = config['special_gemweb_data']
            #gemweb.gemweb.connection(connection['username'], connection['password'], timezone="UTC")
            mongo_logger.log("log in to gemweb API successful")
        except Exception as e:
            mongo_logger.log(f"log in to gemweb API error: {e}")
            continue
        data = {}
        for t in data_types:
            mongo_logger.log(f"obtaining gemweb data from entity {t}")
            try:
                data[t] = get_data(gemweb, data_types[t])
                mongo_logger.log(f"gemweb data from entity {t} obtained successfully")
            except Exception as e:
                mongo_logger.log(f"gemweb data from entity {t} obtained error: {e}")
        if args.store == "k":
            mongo_logger.log(f"sending data to kafka store topic")
            k_store_topic = config["datasources"]["gemweb"]["kafka_store_topic"]
            for d_t in data:
                chunks = range(0, len(data[d_t]), settings.kafka_message_size)
                mongo_logger.log(f"sending {d_t} data to store topic")
                for num, i in enumerate(chunks):
                    message_part = f"{num + 1}/{len(chunks)}"
                    try:
                        mongo_logger.log(f"sending {message_part} part")
                        chunk = data[d_t][i:i + settings.kafka_message_size]
                        kafka_message = {
                            "namespace": connection["namespace"],
                            "user": connection["user"],
                            "collection_type": d_t,
                            "source": "gemweb",
                            "row_keys": ["id"],
                            "logger": mongo_logger.export_log(),
                            "message_part": message_part,
                            "data": chunk
                        }
                        save_to_kafka(topic=k_store_topic, info_document=kafka_message, config=config['kafka'])
                        mongo_logger.log(f"part {message_part}  sent correctly")
                    except Exception as e:
                        mongo_logger.log(f"error when sending part {message_part}: {e}")
            mongo_logger.log(f"sending data to kafka harmonize topic")
            if any(['buildings' not in data, 'supplies' not in data]):
                mongo_logger.log(f"not enough data to harmonize {list(data.keys())}")
                continue
            try:
                supplies_df = pd.DataFrame.from_records(data['supplies'])
                buildings_df = pd.DataFrame.from_records(data['buildings'])
                buildings_df.set_index("id", inplace=True)
                df = supplies_df.join(buildings_df, on='id_centres_consum', lsuffix="supply", rsuffix="building")
                df.rename(columns={"id":"dev_gem_id"}, inplace=True)
                mongo_logger.log(f"data joined for harmonization correctly")
            except Exception as e:
                mongo_logger.log(f"error joining dataframes: {e}")
                continue
            k_harmonize_topic = config["datasources"]["gemweb"]["kafka_harmonize_topic"]
            chunks = range(0, len(df), settings.kafka_message_size)
            mongo_logger.log(f"sending data to harmonize topic")
            data = df.to_dict(orient="records")
            for num, i in enumerate(chunks):
                message_part = f"{num + 1}/{len(chunks)}"
                try:
                    mongo_logger.log(f"sending {message_part} part")
                    chunk = data[i:i + settings.kafka_message_size]
                    kafka_message = {
                        "namespace": connection["namespace"],
                        "user": connection["user"],
                        "collection_type": "harmonize",
                        "source": "gemweb",
                        "row_keys": ["id"],
                        "logger": mongo_logger.export_log(),
                        "message_part": message_part,
                        "data": chunk
                    }
                    save_to_kafka(topic=k_harmonize_topic, info_document=kafka_message, config=config['kafka'])
                    mongo_logger.log(f"part {message_part}  sent correctly")
                except Exception as e:
                    mongo_logger.log(f"error when sending part {message_part}: {e}")

        elif args.store == "h":
            for d_t in data:
                mongo_logger.log(f"saving {d_t} data to hbase")
                try:
                    h_table_name = f"{config['datasources']['gemweb']['hbase_table']}_{d_t}_{connection['user']}"
                    save_to_hbase(data[d_t], h_table_name, config['hbase_imported_data'], [("info", "all")],
                                  row_fields=["id"])
                    mongo_logger.log(f"successfully saved to hbase")
                except Exception as e:
                    mongo_logger.log(f"error saving to hbase: {e}")
