import json
import sys

import pandas
from pymongo import MongoClient


def camelCase(st):
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]


if __name__ == '__main__':
    """
    :arg 1 csv file
    :arg 2 building_id ej. C0:D3:91:32:E9:B1 - 17089108
    :arg 3 building_name ej. Primer de Maig
    """

    if len(sys.argv) < 3:
        print("python csv_file building_id building_name")
        exit(-1)

    # Set Config
    with open('config.json', 'r') as file:
        config = json.load(file)

    # MongoDB Connection
    mongodb_config = config['mongo_db']
    URI = 'mongodb://%s:%s@%s:%s/%s' % (
        mongodb_config['user'], mongodb_config['password'], mongodb_config['host'],
        mongodb_config['port'],
        mongodb_config['db'])

    mongo_connection = MongoClient(URI)
    db = mongo_connection[mongodb_config['db']]
    collection = db['ixon_devices']

    # Read CSV
    df = pandas.read_csv(sys.argv[1])

    df['Object ID'] = df['Object ID'].apply(lambda x: int(x.strip().split(',')[1][:-1]))
    df['BACnet Type'] = df['BACnet Type'].apply(lambda i: camelCase(i.replace('-', ' ')))
    df['building_id'] = sys.argv[2]
    df['building_name'] = sys.argv[3]
    df.rename(
        columns={'Object ID': 'object_id', 'BACnet Type': 'type', 'BACnet Name': 'name', 'Description': 'description'},
        inplace=True)

    collection.insert_many(json.loads(df.to_json(orient='records')))

    exit(0)
