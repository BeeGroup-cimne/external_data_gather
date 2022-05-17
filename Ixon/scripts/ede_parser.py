import argparse
import json

import pandas as pd

from utils import get_json_config, connection_mongo

bacnet_type = {'0': 'analogInput', '1': 'analogOutput', '2': 'analogValue', '3': 'binaryInput', '4': 'binaryOutput',
               '5': 'binaryValue', '13': 'multiStateInput', '14': 'multiStateOutput', '19': 'multiStateValue'}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--file", required=True, type=str)
    parser.add_argument("-bi", "--building_id", required=True, type=str)
    parser.add_argument("-bn", "--building_name", required=True, type=str)
    parser.add_argument("-ip", "--ip", required=True, type=str)
    parser.add_argument("-bii", "--building_internal_id", required=True, type=str)
    args = parser.parse_args()

    # MongoDB Connection
    config = get_json_config('../config.json')
    db = connection_mongo(config['mongo_db'])

    collection = db['ixon_devices']

    # Read CSV
    input_path = args.file
    df = pd.read_csv(input_path, sep=';', encoding='utf-8', skiprows=7)  # Take care encoding

    # Reduce dataset
    df = df[['# keyname', 'object-type', 'object-instance', 'description']]
    df['building_id'] = args.building_id
    df['building_name'] = args.building_name

    # Rename Column names
    df.rename(
        columns={'object-instance': 'object_id', 'object-type': 'type', '# keyname': 'name',
                 'description': 'description'},
        inplace=True)

    df['type'] = df['type'].apply(lambda i: bacnet_type.get(str(i)))
    df['bacnet_device_ip'] = args.ip
    df['building_internal_id'] = args.building_internal_id
    df.dropna(subset=['type'], inplace=True)

    collection.insert_many(json.loads(df.to_json(orient='records')))
