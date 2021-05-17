import pymongo


def save_to_mongo(mongo, documents, index_field=None):
    documents_ = documents.copy()
    if index_field:
        for d in documents_:
            d['_id'] = d.pop(index_field)
    if documents:
        mongo.remove()
        mongo.insert_many(documents_)


def save_to_hbase(documens, index_field=None):
    pass


