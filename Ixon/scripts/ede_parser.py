import argparse

import pandas as pd

from utils import get_json_config, connection_mongo

bacnet_type = {'0': 'analogInput', '1': 'analogOutput', '2': 'analogValue', '3': 'binaryInput', '4': 'binaryOutput',
               '5': 'binaryValue', '13': 'multiStateInput', '14': 'multiStateOutput', '19': 'multiStateValue'}

COLUMNS = ['# keyname', 'device-object-instance', 'object-name', 'object-type', 'object-instance', 'description',
           'present-value-default', 'min-present-value', 'max-present-value', 'settable', 'supports COV', 'hi-limit',
           'low-limit', 'state text reference', 'unit-code', 'vendor-specific-address']
if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--file", required=True, type=str)
    parser.add_argument("-ip", "--ip", required=True, type=str)
    parser.add_argument("-id", "--id", required=True, type=str)
    args = parser.parse_args()

    # MongoDB Connection
    config = get_json_config('../config.json')
    db = connection_mongo(config['mongo_db'])

    buildings_info = get_json_config('../buildings.json')

    current_building = buildings_info[args.id]

    collection = db['ixon_devices']

    # Read CSV
    input_path = args.file

    if '.csv' in input_path:
        try:
            df = pd.read_csv(input_path, sep=';', encoding='utf-8', skiprows=7, header=None,
                             names=COLUMNS)  # Take care encoding
        except:
            df = pd.read_csv(input_path, sep=';', encoding='utf-16', skiprows=7, header=None,
                             names=COLUMNS)  # Take care encoding
    else:
        df = pd.read_excel(input_path, skiprows=8, header=None)
        df[COLUMNS] = df[0].str.split(';', expand=True)

    # Reduce dataset
    df = df[['# keyname', 'object-type', 'object-instance', 'description']]
    df['building_id'] = current_building['deviceId']
    df['building_name'] = current_building['name']

    # Rename Column names
    df.rename(
        columns={'object-instance': 'object_id', 'object-type': 'type', '# keyname': 'name',
                 'description': 'description'},
        inplace=True)
    df['type'] = df['type'].apply(lambda i: bacnet_type.get(str(i)))
    df['bacnet_device_ip'] = args.ip
    df['building_internal_id'] = args.id
    df.dropna(subset=['type'], inplace=True)

    # collection.insert_many(json.loads(df.to_json(orient='records')))

    # for index, row in df.iterrows():
    #     collection.delete_one({"building_name": row['building_name'], "building_id": row['building_id'],
    #                            "name": row['name'],
    #                            "type": row["type"], "object_id": row["object_id"]})
    # {"$set": {"bacnet_device_ip": row['bacnet_device_ip'],
    #           "building_internal_id": row['building_internal_id']}},
    # upsert=False)
