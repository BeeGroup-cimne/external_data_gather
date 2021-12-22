import base64
import hashlib
import json
import os
import subprocess
import time
import uuid

import happybase
from Crypto.Cipher import AES
from pymongo import MongoClient


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
def connection_hbase(config):
    if 'db' in config and config['db'] != "":
        hbase = happybase.Connection(config['host'], config['port'], table_prefix=config['db'],
                                     table_prefix_separator=":")
    else:
        hbase = happybase.Connection(config['host'], config['port'])
    hbase.open()
    return hbase


def get_HTable(hbase, table_name, cf=None):
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


def save_to_hbase(HTable, documents, cf_mapping, row_fields=None, version=int(time.time()), batch_size=1000):
    htbatch = HTable.batch(timestamp=version, batch_size=batch_size)
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
            htbatch.put(str(row), values)
    htbatch.send()


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
