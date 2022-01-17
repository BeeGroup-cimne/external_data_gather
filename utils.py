import json
import uuid
from copy import deepcopy

from kafka import KafkaProducer
from pymongo import MongoClient
import happybase
import time


# MongoDB functions
def connection_mongo(config):
    cli = MongoClient("mongodb://{user}:{pwd}@{host}:{port}/{db}".format(**config))
    db = cli[config['db']]
    return db


def save_to_mongo(mongo, documents, index_field=None):
    documents_ = documents.copy()
    if index_field:
        for d in documents_:
            d['_id'] = d.pop(index_field)
    if documents:
        mongo.remove()
        mongo.insert_many(documents_)


# HBase functions

def __get_h_table__(hbase, table_name, cf=None):
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass
        else:
            print(e)
    return hbase.table(table_name)


def save_to_hbase(documents, h_table_name, hbase_connection, cf_mapping, row_fields=None,
                  version=int(time.time()), batch_size=1000):
    hbase = happybase.Connection(**hbase_connection)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(timestamp=version, batch_size=batch_size)
    row_auto = 0
    uid = uuid.uuid4()
    for d in documents:
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d.pop(f)) if f in d else "" for f in row_fields])
        values = {}
        for cf, fields in cf_mapping:
            if fields == "all":
                for c, v in d.items():
                    values["{cf}:{c}".format(cf=cf, c=c)] = str(v)
            else:
                for c in fields:
                    if c in d:
                        values["{cf}:{c}".format(cf=cf, c=c)] = str(d[c])
        h_batch.put(str(row), values)
    h_batch.send()


def save_to_kafka(topic, info_document, config, batch=1000):
    info_document = deepcopy(info_document)
    servers = [f"{host}:{port}" for host, port in zip(config['hosts'], config['ports'])]
    producer = KafkaProducer(bootstrap_servers=servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             compression_type='gzip')
    data_message = info_document.pop("data")
    while data_message:
        send_message = deepcopy(info_document)
        send_message["data"] = data_message[:batch]
        data_message = data_message[batch:]
        f = producer.send(topic, value=send_message)
        f.get(timeout=10)

# def save_to_kafka(topic, info_document, config):
#     """
#     Creates a set of messages to be sent to kafka. This messages will consist of an inicial message for type with metadata.
#     The set of different parts of data to send of each kind, and a final message to indicate the ending of the sequence.
#     All messages will be sent using the same message_id. so they go always to same partition and can be identified.
#     :param topic: the topic where to publish the message
#     :param info_document: a list of message types. each message type is a dictionary with some meta fields and a "data"
#      field with the information. If a batch info is in the meta, this will be used to split the information
#     :param config: The kafka host configutration.
#     :return:
#     """
#     info_document = deepcopy(info_document)
#     servers = [f"{host}:{port}" for host, port in zip(config['hosts'], config['ports'])]
#     producer = KafkaProducer(bootstrap_servers=servers,
#                              value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#                              key_serializer=lambda v: v.encode('utf-8'),
#                              compression_type='gzip')
#     message_id = str(uuid.uuid4())
#     for info in info_document:
#         data_message = info.pop("data")
#         info["message_type"] = "meta"
#         batch = info.pop("batch") if "batch" in info else 1000
#         f = producer.send(topic, key=message_id, value=info)
#         f.get(timeout=20)
#         while data_message:
#             send_message = {
#                 "message_type": "data",
#                 "data": data_message[:batch]
#             }
#             data_message = data_message[batch:]
#             f = producer.send(topic, key=message_id, value=send_message)
#             f.get(timeout=20)
#     final_message = {
#         "message_type": "end"
#     }
#     f = producer.send(topic, key=message_id, value=final_message)
#     return f.get(timeout=20)
