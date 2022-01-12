import argparse
from datetime import datetime

import happybase
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from utils import connection_mongo, get_json_config

sns.set_theme(style="whitegrid")


def loss_period_bar_plot(data_init, data_end):
    values = []
    for building_name in building_names:
        for i in pd.date_range(data_init, data_end, freq="1D"):
            aux_end = i.replace(hour=23, minute=59, second=59)

            total_logs = device_logs.count_documents(
                {"building_name": building_name, "date": {"$gte": i,
                                                          "$lt": aux_end}})
            fail_logs = device_logs.count_documents(
                {"building_name": building_name, "date": {"$gte": i,
                                                          "$lt": aux_end},
                 "successful": False})

            values.append([building_name, i.date().strftime("%d/%b"), round(fail_logs * 100 / total_logs, 2)])

    df = pd.DataFrame(values, columns=['building', 'date', 'val'])

    plt.figure(figsize=(10, 10), dpi=80)
    g = sns.catplot(data=df, kind="bar", x="date", y="val", ci=None, hue="building")
    sns.set(font_scale=0.5)
    g.despine(left=True)
    g.set_axis_labels("", "Loss Rate (%)")
    g.legend.set_title("Building Name")
    plt.show()


def loss_period_signal_plot(data_init, data_end):
    values = []
    for building_name in building_names:
        for i in pd.date_range(data_init, data_end, freq="1D"):
            aux_end = i.replace(hour=23, minute=59, second=59)

            total_logs = device_logs.find(
                {"building_name": building_name, "date": {"$gte": i,
                                                          "$lt": aux_end}},
                {"building_name": 1, "date": 1, "_id": 0, "successful": 1})
            values += list(total_logs)

    df = pd.DataFrame(values)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%H:%M')

    plt.figure(figsize=(10, 10), dpi=80)
    sns.set(font_scale=0.5)
    g = sns.lineplot(data=df, x="date", y="successful", hue="building_name")

    plt.xticks(rotation=90)
    plt.show()


def device_plot(devices, data_init, data_end):
    for x in devices:
        table = "ixon_data_infraestructures"
        hbase = happybase.Connection(**config['happybase_con'])
        h_table = hbase.table(table)
        list1 = []

        for k, d in h_table.scan(
                row_start=f"{x['building_id']}~{x['name']}~{int(data_init.timestamp())}".encode("utf-8"),
                row_stop=f"{x['building_id']}~{x['name']}~{int(data_end.timestamp())}".encode("utf-8")):
            b, d_, ts = k.decode("utf-8").split("~")
            ts = datetime.utcfromtimestamp(float(ts))
            try:
                value = float(d[b'v:value'].decode())
            except:
                value = np.NAN
            list1.append({"ts": ts, "v": value})

        if len(list1) <= 0:
            print("nodata")
            continue

        df = pd.DataFrame.from_records(list1)
        df.set_index("ts", inplace=True)
        df.sort_index()
        df = df.resample("15T").mean()
        fig = plt.figure(figsize=(15, 10), dpi=80)
        fig.suptitle(d_)
        plt.plot(df)
        # fig.show()
        plt.savefig('./reports/IES Marta Mata/' + d_ + '.png')


if __name__ == '__main__':
    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--type", required=True, type=str, help="Available types: day, signal")
    parser.add_argument("-i", "--init", required=True, type=str, help="Date with the format: yyyy/mm/dd, 2021/05/23")
    parser.add_argument("-f", "--final", required=True, type=str, help="Date with the format: yyyy/mm/dd, 2021/05/23")
    parser.add_argument("-b", "--buildings", type=str, default="all",
                        help="Buildings name/id divided by coma or all buildings")

    args = parser.parse_args()

    config = get_json_config('../config.json')
    db = connection_mongo(config['mongo_db'])
    device_logs = db['ixon_logs']
    ixon_devices = db['ixon_devices']
    building_names = []
    building_ids = []

    data_init = datetime.strptime(args.init, "%Y/%m/%d")
    data_end = datetime.strptime(args.final, "%Y/%m/%d")

    if args.buildings == 'all':
        building_names = device_logs.distinct('building_name')
        building_ids = device_logs.distinct('building_id')
    else:
        building_names = args.buildings.split(',')
        building_ids = args.buildings.split(',')

    if args.type == 'day':
        loss_period_bar_plot(data_init, data_end)

    if args.type == 'signal':
        loss_period_signal_plot(data_init, data_end)

    if args.type == 'devices':
        for building_id in building_ids:
            devices = ixon_devices.find({"building_id": building_id})
            device_plot(list(devices), data_init, data_end)
