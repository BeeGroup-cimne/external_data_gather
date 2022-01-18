import pickle
import base64
import hashlib
import json
import os
import subprocess
import time
import uuid
from copy import deepcopy
from datetime import datetime
from kafka import KafkaProducer
import happybase
from Crypto.Cipher import AES
from pymongo import MongoClient


def read_config(conf_file):
    with open(conf_file) as config_f:
        config = json.load(config_f)
        return config


class mongo_logger(object):
    mongo_conf = None
    collection = None

    log_id = None
    db = None
    log_type = None

    @staticmethod
    def __connect__(mongo_conf, collection):
        mongo_logger.mongo_conf = mongo_conf
        mongo_logger.collection = collection
        mongo = mongo_logger.connection_mongo(mongo_logger.mongo_conf)
        mongo_logger.db = mongo[mongo_logger.collection]
    @staticmethod
    def create(mongo_conf, collection, log_type,  user):
        mongo_logger.__connect__(mongo_conf, collection)
        mongo_logger.log_type = log_type
        log_document = {
            "user": user,
            "logs": {
                "gather": [],
                "store": [],
                "harmonize": []
            }
        }
        mongo_logger.log_id = mongo_logger.db.insert_one(log_document).inserted_id

    @staticmethod
    def export_log():
        return {
            "mongo_conf": mongo_logger.mongo_conf,
            "collection": mongo_logger.collection,
            "log_id": mongo_logger.log_id
        }

    @staticmethod
    def import_log(exported_info, log_type):
        mongo_logger.__connect__(exported_info['mongo_conf'], exported_info['collection'])
        mongo_logger.log_id = exported_info['log_id']
        mongo_logger.log_type = log_type

    @staticmethod
    def log(message):
        if any([mongo_logger.db is None, mongo_logger.db is None, mongo_logger.log_type is None]):
            return
        mongo_logger.db.update_one({"_id": mongo_logger.log_id},
                                   {"$push": {
                                       f"logs.{mongo_logger.log_type}": f"{datetime.utcnow()}: \
                                       {message}"}})

    # MongoDB functions
    @staticmethod
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
                             value_serializer=lambda v: pickle.dumps(v),
                             compression_type='gzip')
    data_message = info_document.pop("data")
    while data_message:
        send_message = deepcopy(info_document)
        send_message["data"] = data_message[:batch]
        data_message = data_message[batch:]
        f = producer.send(topic, value=send_message)
        f.get(timeout=10)


def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):

    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1]+chr(row_prefix[-1]+1).encode("utf-8")

    if limit:
        if limit > batch_size:
            current_limit = batch_size
        else:
            current_limit = limit
    else:
        current_limit = batch_size
    current_register = 0
    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)
        data = list(table.scan(row_start=row_start, row_stop=row_stop, columns=columns, filter=_filter,
                               timestamp=timestamp, include_timestamp=include_timestamp, batch_size=batch_size,
                               scan_batching=scan_batching, limit=current_limit, sorted_columns=sorted_columns,
                               reverse=reverse))
        if not data:
            break
        last_record = data[-1][0]
        current_register += len(data)
        yield data

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)
        row_start = last_record[:-1] + chr(last_record[-1] + 1).encode("utf-8")
    yield []


def un_pad(s):
    """
    remove the extra spaces at the end
    :param s:
    :return:
    """
    return s.rstrip()


def decrypt(enc_dict, password):
    # decode the dictionary entries from base64
    salt = base64.b64decode(enc_dict['salt'])
    enc = base64.b64decode(enc_dict['cipher_text'])
    iv = base64.b64decode(enc_dict['iv'])

    # generate the private key from the password and salt
    private_key = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)

    # create the cipher config
    cipher = AES.new(private_key, AES.MODE_CBC, iv)

    # decrypt the cipher text
    decrypted = cipher.decrypt(enc)

    # unpad the text to remove the added spaces
    original = un_pad(decrypted)

    return original


def get_json_config(path):
    file = open(path, "r")
    return json.load(file)


def put_file_to_hdfs(source_file_path, destination_file_path):
    output = subprocess.call(f"hdfs dfs -put -f {source_file_path} {destination_file_path}", shell=True)
    return destination_file_path + source_file_path.split('/')[-1]


def remove_file_from_hdfs(file_path):
    output = subprocess.call(f"hdfs dfs -rm {file_path}", shell=True)


def remove_file(file_path):
    os.remove(file_path)
