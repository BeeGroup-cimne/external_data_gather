import argparse
import os.path
from datetime import datetime

import happybase
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from utils import connection_mongo, get_json_config


def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


def loss_period_bar_plot(buildings, buildings_type, date_init, date_end):
    for building in buildings:
        values = []
        for i in pd.date_range(date_init, date_end, freq="1D"):
            aux_date_end = i.replace(hour=23, minute=59, second=59)

            total_logs = device_logs.count_documents(
                {f"building_{buildings_type}": building, "date": {"$gte": i,
                                                                  "$lt": aux_date_end}})
            fail_logs = device_logs.count_documents(
                {f"building_{buildings_type}": building, "date": {"$gte": i,
                                                                  "$lt": aux_date_end},
                 "successful": False})

            print(f"{building}: Total: {total_logs}, Success: {total_logs - fail_logs}, Fail: {fail_logs}\n")

            try:
                loss_rate = 0 if fail_logs == 0 else round(fail_logs * 100 / total_logs, 2)
                values.append([building, i.date().strftime("%d/%b/%y"), loss_rate])

            except Exception as ex:
                print(ex)

        df = pd.DataFrame(data=values, columns=[f"building_{buildings_type}", 'date', 'loss_rate'])

        df.plot(kind='bar', x="date", y='loss_rate')

        plt.xlabel('Days')
        plt.ylabel('Loss Rate')
        plt.title(building)

        plt.xticks(rotation=45)
        plt.ylim(0)
        plt.ticklabel_format(style='plain', axis='y')
        plt.tight_layout()
        create_folder(f'reports/loss_rate/')
        plt.savefig(f'./reports/loss_rate/{building}.png')


def loss_period_signal_plot(buildings, buildings_type, date_init, date_end):
    aggregate_buildings = []
    create_folder(f'reports/loss_period/')
    for building in buildings:
        values = []
        for i in pd.date_range(date_init, date_end, freq="1D"):
            aux_end = i.replace(hour=23, minute=59, second=59)

            total_logs = device_logs.find(
                {f"building_{buildings_type}": building, "date": {"$gte": i, "$lt": aux_end}},
                {f"building_{buildings_type}": 1, "date": 1, "_id": 0, "successful": 1})
            values += list(total_logs)

        df = pd.DataFrame(values)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%H:%M')
        aggregate_buildings.append(df)

        sns.set(font_scale=0.5)
        g = sns.lineplot(data=df, x="date", y="successful", hue=f"building_{buildings_type}")
        plt.xticks(rotation=45)
        plt.ylim(0)
        plt.ticklabel_format(style='plain', axis='y')
        plt.tight_layout()
        plt.savefig(f'./reports/loss_rate/{building}.png')

    df = pd.concat(aggregate_buildings)
    fig, ax = plt.subplots()

    for name in buildings:
        ax.plot(df[df[f"building_{buildings_type}"] == name].date,
                df[df[f"building_{buildings_type}"] == name].successful, label=name)

    ax.set_xlabel("Date")
    ax.set_ylabel("Successful")
    ax.legend(loc='best')
    fig.savefig(f'./reports/loss_rate/aggregate_buildings.png')


def device_plot(devices, date_init, date_end):
    for device in devices:
        table = "raw_data:ixon_data_infraestructures"
        hbase = happybase.Connection(**config['happybase_con'])
        h_table = hbase.table(table)
        list1 = []

        for k, d in h_table.scan(
                row_start=f"{device['building_id']}~{device['name']}~{int(date_init.timestamp())}".encode("utf-8"),
                row_stop=f"{device['building_id']}~{device['name']}~{int(date_end.timestamp())}".encode("utf-8")):
            b, d_, ts = k.decode("utf-8").split("~")
            ts = datetime.utcfromtimestamp(float(ts))
            try:
                value = float(d[b'v:value'].decode())
            except:
                value = np.NAN
            list1.append({"ts": ts, "v": value})

        if len(list1) <= 0:
            continue

        df = pd.DataFrame.from_records(list1)
        df.set_index("ts", inplace=True)
        df.sort_index()

        df = df.resample("15T").mean()
        df.plot(kind='line')

        plt.xticks(rotation=45)
        plt.ticklabel_format(style='plain', axis='y')
        plt.title(d_)
        plt.tight_layout()

        create_folder(f'reports/devices_ts/{devices[0]["building_name"]}')
        plt.savefig(f'reports/devices_ts/{devices[0]["building_name"]}/{d_}.png')


def ranking_loses(buildings_type, date_init, date_end):
    results = list(device_logs.aggregate([{"$match": {"date": {"$gte": date_init,
                                                               "$lt": date_end}, "successful": False}},
                                          {"$group": {"_id": f"$building_{buildings_type}", "count": {"$sum": 1}}},
                                          {"$sort": {"count": -1}}]))
    if results:
        df = pd.DataFrame(results)
        df.plot(kind='bar', x="_id", y='count')
        plt.xticks(rotation=45)
        plt.ticklabel_format(style='plain', axis='y')
        plt.tight_layout()

        create_folder(f'reports/loss_ranking/')
        plt.savefig(f'reports/loss_ranking/{building}_{date_init}_{date_end}.png')


def network_daily_traffic(buildings, data_init, data_end):
    for id in buildings:
        l = []
        for i in pd.date_range(data_init, data_end, freq="1D"):
            try:
                aux_end = i.replace(hour=23, minute=59, second=59)
                list_values = list(
                    network_usage.find({"building": id['building_id'], "timestamp": {"$gte": i, "$lt": aux_end}},
                                       {"bytes_recv": 1, "bytes_sent": 1, "_id": 0}))
                if list_values:
                    df = pd.DataFrame().from_records(list_values)

                    x = df.sum()
                    l.append({"date": i.date(), "bytes_sent": x['bytes_sent'], 'bytes_recv': x['bytes_recv']})
            except:
                pass

        if l:
            df = pd.DataFrame(l)
            df.set_index('date', inplace=True)

            ax = df.plot(stacked=True, kind='bar')

            plt.xlabel('Days')
            plt.ylabel('Bytes')
            plt.title(id['building_name'])

            plt.xticks(rotation=45)
            plt.ticklabel_format(style='plain', axis='y')
            plt.tight_layout()

            create_folder(f'reports/network_aggregate_daily_traffic/')
            plt.savefig(f'reports/network_aggregate_daily_traffic/{id["building_name"]}.png')


def network_usage_plot(buildings, date_init, date_end):
    for id in buildings:
        list_values = list(
            network_usage.find({"building": id['building_id'], "timestamp": {"$gte": date_init, "$lt": date_end}},
                               {"bytes_recv": 1, "bytes_sent": 1, "_id": 0, "timestamp": 1}))
        if list_values:
            df = pd.DataFrame.from_records(list_values)
            df.set_index('timestamp', inplace=True)
            df.plot()

            plt.xlabel('Days')
            plt.ylabel('Bytes')
            plt.title(id)

            plt.xticks(rotation=45)
            plt.ticklabel_format(style='plain', axis='y')
            plt.tight_layout()

            create_folder(f'reports/network_traffic/')
            plt.savefig(f'reports/network_traffic/{id["building_name"]}_{date_init}_{date_end}.png')


def network_traffic_per_devices(buildings, date_init, date_end):
    _buildings = [ixon_devices.find_one({f"building_{args.buildings_type}": building},
                                        {'building_name': 1, 'building_id': 1, '_id': 0}) for building in buildings]

    for building in _buildings:
        data = []
        for i in pd.date_range(date_init, date_end, freq="1D"):
            try:
                aux_end = i.replace(hour=23, minute=59, second=59)
                list_values = list(
                    network_usage.find({"building": building['building_id'], "timestamp": {"$gte": i, "$lt": aux_end}},
                                       {"bytes_recv": 1, "bytes_sent": 1, "_id": 0}))
                if list_values:
                    df = pd.DataFrame().from_records(list_values)
                    x = df.sum()
                    data.append({"date": i.date(),
                                 "total": (x['bytes_sent'] + x['bytes_recv']) / ixon_devices.count_documents(
                                     {"building_id": building['building_id']})})
            except:
                pass


if __name__ == '__main__':
    # Arguments
    parser = argparse.ArgumentParser()
    type_list = ['loss_period', 'network_aggregate_daily_traffic', 'network_traffic', 'devices_data', 'loss_ranking']

    parser.add_argument("-t", "--type", required=True, type=str, choices=type_list,
                        help="Type of analysis that you want")

    parser.add_argument("-i", "--init_date", required=True, type=str,
                        help="Date with the format: yyyy/mm/dd, 2021/05/23")

    parser.add_argument("-e", "--end_date", required=True, type=str,
                        help="Date with the format: yyyy/mm/dd, 2021/05/23")

    parser.add_argument("-bt", "--buildings_type", choices=['id', 'name'], type=str, default="all",
                        help="Buildings name divided by coma or all buildings")

    parser.add_argument("-b", "--buildings", type=str, default="all",
                        help="Buildings name divided by coma or all buildings")

    args = parser.parse_args()

    # Database Config
    config = get_json_config('/Users/francesc/Desktop/external_data_gather/Ixon/config.json')
    db = connection_mongo(config['mongo_db'])

    device_logs = db['ixon_logs']
    ixon_devices = db['ixon_devices']
    network_usage = db['network_usage']

    # Parse date
    date_init = datetime.strptime(args.init_date, "%Y/%m/%d")
    date_end = datetime.strptime(args.end_date, "%Y/%m/%d")

    buildings = []

    if args.buildings == 'all':
        if args.buildings_type == 'id':
            buildings = ixon_devices.distinct('building_id')
        else:
            buildings = ixon_devices.distinct('building_name')
    else:
        buildings = args.buildings.split(',')

    if args.type == 'loss_period':
        loss_period_bar_plot(buildings, args.buildings_type, date_init, date_end)
        loss_period_signal_plot(buildings, args.buildings_type, date_init, date_end)

    if args.type == 'devices_data':
        for building in buildings:
            devices = list(ixon_devices.find({f"building_{args.buildings_type}": building}, {'_id': 0}))
            device_plot(list(devices), date_init, date_end)

    if args.type == 'loss_ranking':
        ranking_loses(args.buildings_type, date_init, date_end)

    if args.type == 'network_aggregate_daily_traffic':
        _buildings = [ixon_devices.find_one({f"building_{args.buildings_type}": building},
                                            {'building_name': 1, 'building_id': 1, '_id': 0}) for building in buildings]
        network_daily_traffic(_buildings, date_init, date_end)

    if args.type == 'network_traffic':
        _buildings = [ixon_devices.find_one({f"building_{args.buildings_type}": building},
                                            {'building_name': 1, 'building_id': 1, '_id': 0}) for building in buildings]
        network_usage_plot(_buildings, date_init, date_end)

    if args.type == 'network_traffic_devices':
        network_traffic_per_devices(buildings, date_init, date_end)
