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
    for d in documents:
        if not row_fields:
            row = row_auto
            row_auto += 1
        else:
            row = "~".join([str(d.pop(f) ) if f in d else "" for f in row_fields])
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



