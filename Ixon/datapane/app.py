import argparse
from datetime import datetime, timedelta

import datapane as dp
import pandas as pd
import plotly.express as px

from utils import get_json_config, connection_mongo


def generate_network_usage(date_init, date_end):
    df = pd.DataFrame(list(network_usage.find(
        {"timestamp": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}},
        {"bytes_recv": 1, "bytes_sent": 1, "_id": 0, "timestamp": 1, "building": 1})))

    df['total_KBytes'] = (df['bytes_recv'] + df['bytes_sent']) / 1000
    df['building'] = df['building'].map(buildings)

    return df


def get_buildings():
    aux = list(devices.aggregate([
        {"$group": {
            "_id": {
                "building_name": "$building_name",
                "building_id": "$building_id"
            }
        }
        }, {"$project":
                {"name": "$_id.building_name",
                 "id": "$_id.building_id",
                 "_id": 0}
            }
    ]))

    res = {}
    for i in aux:
        res.update({i['id']: i['name']})

    return res


def get_num_devices():
    num_devices = {}
    for key in get_buildings().keys():
        num_devices.update({key: devices.count_documents({"building_id": key})})
    return num_devices


def generate_network_usage_per_device(date_init, date_end):
    num_devices = get_num_devices()
    df = pd.DataFrame(list(network_usage.find({"timestamp": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}},
                                              {"bytes_recv": 1, "bytes_sent": 1, "_id": 0, "timestamp": 1,
                                               "building": 1})))

    df['num_devices'] = df['building']
    df['num_devices'] = df['num_devices'].map(num_devices)

    df['building'] = df['building'].map(buildings)
    df['total'] = (df['bytes_sent'] + df['bytes_recv']) / 1000

    df['device_mean'] = df['total'] / df['num_devices']

    df_1 = df[df["num_devices"] <= 250]
    df_2 = df[(df["num_devices"] <= 500) & (df["num_devices"] > 250)]
    df_3 = df[(df["num_devices"] <= 750) & (df["num_devices"] > 500)]
    df_4 = df[(df["num_devices"] <= 1000) & (df["num_devices"] > 750)]
    df_5 = df[(df["num_devices"] > 1000)]

    data = [{"label": "Rang (0,250] dispositius", "data": df_1},
            {"label": "Rang (250,500] dispositius", "data": df_2},
            {"label": "Rang (500,750] dispositius", "data": df_3},
            {"label": "Rang (750,1000] dispositius", "data": df_4},
            {"label": "Rang (1000,∞] dispositius", "data": df_5},
            ]

    x = []
    for i in data:
        if i['data'].shape[0]:
            network_usage_per_device_plot = px.line(i['data'], x='timestamp', y='device_mean',
                                                    color='building',
                                                    title=i['label'],
                                                    labels={"building": "Edificis", "device_mean": "dispositiu/KBytes",
                                                            "timestamp": "data"}, hover_name="building",
                                                    hover_data=["device_mean", "num_devices"])
            x.append(dp.Plot(network_usage_per_device_plot))
    return x


def generate_network_usage_total_per_day(date_init, date_end):
    df = pd.DataFrame(list(network_usage.find({"timestamp": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}},
                                              {"bytes_recv": 1, "bytes_sent": 1, "_id": 0, "timestamp": 1,
                                               "building": 1})))

    df['total'] = (df['bytes_recv'] + df['bytes_sent']) / 1000
    df['building'] = df['building'].map(buildings)
    df.set_index('timestamp', inplace=True)
    return df.groupby('building').resample('D').sum()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--init_date", required=True, type=str,
                        help="Date with the format: yyyy/mm/dd, 2021/05/23")

    parser.add_argument("-e", "--end_date", required=True, type=str,
                        help="Date with the format: yyyy/mm/dd, 2021/05/23")

    args = parser.parse_args()

    date_init = datetime.strptime(args.init_date, "%Y/%m/%d")
    date_end = datetime.strptime(args.end_date, "%Y/%m/%d")

    # MongoDB Connection
    config = get_json_config('../config.json')
    db = connection_mongo(config['mongo_db'])

    logs = db['ixon_logs']
    devices = db['ixon_devices']
    network_usage = db['network_usage']

    # Get buildings
    buildings = get_buildings()

    # Network Usage Plot
    df_network_usage = generate_network_usage(date_init=date_init, date_end=date_end)

    network_usage_plot = px.line(df_network_usage, x='timestamp', y="total_KBytes", color="building",
                                 labels={"building": "Edificis", "total_KBytes": "KBytes", "timestamp": "data"})

    # Network Usage Per Device Plot

    dp_network_usage_per_device = generate_network_usage_per_device(
        date_init=date_init, date_end=date_end)

    df_network_usage_total_per_day = generate_network_usage_total_per_day(date_init=date_init,
                                                                          date_end=date_end)

    df_network_usage_total_per_day_plot = px.bar(df_network_usage_total_per_day,
                                                 x=df_network_usage_total_per_day.index.get_level_values(1),
                                                 y="total",
                                                 color=df_network_usage_total_per_day.index.get_level_values(0),
                                                 labels={"building": "Edificis", "total": "KBytes",
                                                         "timestamp": "data"}, barmode='group')

    caption = f"# Informe Setmanal \nData Inci: {date_init.date()}\nData Fi: {date_end.date()}"

    report = dp.Report(
        dp.Text(caption),
        dp.Text("## Tràfic de Dades"),
        dp.Plot(network_usage_plot),
        dp.Text("## Tràfic de Dades Per Dispositiu"),
        *dp_network_usage_per_device,
        dp.Text("## Tràfic de Dades Diari"),
        dp.Plot(df_network_usage_total_per_day_plot)
    )

    # report.save(path='report.html', open=True)
    # report.upload('Report')
