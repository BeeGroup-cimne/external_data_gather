import json
import sys

import pandas as pd
from pymongo import MongoClient

bacnet_type = {'0': 'analogInput', '1': 'analogOutput', '2': 'analogValue', '3': 'binaryInput', '4': 'binaryOutput',
               '5': 'binaryValue', '13': 'multiStateInput', '14': 'multiStateOutput', '19': 'multiStateValue'}

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
    with open('../config.json', 'r') as file:
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
    input_path = str(sys.argv[1])
    df = pd.read_csv(input_path, sep=';', encoding='utf-16', skiprows=7)  # Take care encoding

    # Reduce dataset
    df = df[['#keyname', 'object-type', 'object-instance', 'description']]
    df['building_id'] = str(sys.argv[2])
    df['building_name'] = str(sys.argv[3])

    # Rename Column names
    df.rename(
        columns={'object-instance': 'object_id', 'object-type': 'type', '#keyname': 'name',
                 'description': 'description'},
        inplace=True)

    df['type'] = df['type'].apply(lambda i: bacnet_type.get(str(i)))
    df.dropna(subset=['type'], inplace=True)

    # print(df[df['type'].isnull()])

    collection.insert_many(json.loads(df.to_json(orient='records')))
