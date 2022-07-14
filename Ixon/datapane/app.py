import json
from datetime import timedelta, date, datetime

import datapane as dp
import pandas as pd
import plotly.express as px
from pymongo import MongoClient


def connection_mongo(config):
    cli = MongoClient("mongodb://{user}:{pwd}@{host}:{port}/{db}".format(**config))
    db = cli[config['db']]
    return db


def get_json_config(path):
    file = open(path, "r")
    return json.load(file)


def generate_network_usage(date_init, date_end):
    df = pd.DataFrame(list(network_usage.find(
        {"timestamp": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}},
        {"bytes_recv": 1, "bytes_sent": 1, "_id": 0, "timestamp": 1, "building": 1})))

    df['total_KBytes'] = (df['bytes_recv'] + df['bytes_sent']) / 1000
    df['building'] = df['building'].map(buildings)
    return px.line(df, x='timestamp', y="total_KBytes", color="building",
                   labels={"building": "Edifici/s", "total_KBytes": "KBytes", "timestamp": "Data"})


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
                                                    labels={"building": "Edifici/s", "device_mean": "Disp./KBytes",
                                                            "timestamp": "Data", "num_devices": "Nº Disp."},
                                                    hover_name="building",
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
    df = df.groupby('building').resample('D').sum()
    df = df.round()

    return px.bar(df,
                  x=df.index.get_level_values(1),
                  y="total",
                  color=df.index.get_level_values(0),
                  labels={"color": "Edificis", "total": "KBytes",
                          "timestamp": "Data", "x": "Data"}, barmode='group', text_auto=True)


def generate_request_per_building(date_init, date_end):
    df = pd.DataFrame(list(
        logs.find({"successful": True, "date": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}}, {"_id": 0})))
    df.set_index('date', inplace=True)
    df = df.groupby('building_name').resample('D').sum()

    return px.bar(df,
                  x=df.index.get_level_values(1),
                  y="successful",
                  color=df.index.get_level_values(0),
                  labels={"color": "Edifici/s", "successful": "Peticions amb èxit", "x": "Data",
                          "date": "data"}, barmode='group', text_auto=True)


def generate_request_per_devices(date_init, date_end):
    df = pd.DataFrame(list(logs.aggregate([
        {"$match": {"date": {"$gte": date_init, "$lt": date_end + timedelta(days=1)}}},
        {
            "$project": {
                "successful_req": {
                    "$size": {
                        "$filter": {
                            "input": "$devices_logs",
                            "as": "el",
                            "cond": {"$eq": ["$$el.successful", True]}
                        }
                    }
                },
                "num_devices": {
                    "$cond": {
                        "if": {"$isArray": "$devices_logs"}, "then": {"$size": "$devices_logs"}, "else": 0
                    }
                },
                "building_id": 1,
                "building_name": 1,
                "date": 1,
                "_id": 0
            }
        }
    ])))

    df.set_index("date", inplace=True)
    df.sort_index()
    df['calc'] = (df['successful_req'] * 100) / df['num_devices']
    df = df.groupby('building_name').resample('D').mean()
    df = df.round(1)
    return px.bar(df,
                  x=df.index.get_level_values(1),
                  y="calc",
                  color=df.index.get_level_values(0),
                  labels={"color": "Edifici/s", "calc": "Èxit de recuperació (%)",
                          "x": "Data",
                          "date": "data"}, barmode='group', text_auto=True)


# def send_email():
#     if os.path.exists('report.html'):
#         smtp_server = "smtp.gmail.com"
#         port = 587  # For starttls
#         sender_email = "my@gmail.com"
#         password = input("Type your password and press enter: ")
#
#         context = ssl.create_default_context()


if __name__ == '__main__':
    date_init = date.today() - timedelta(days=date.today().weekday())
    date_init = datetime.combine(date_init, datetime.min.time()) - timedelta(days=7)
    date_end = date_init + timedelta(days=6)

    # date_init = datetime.strptime("2022/05/06", "%Y/%m/%d")
    # date_end = datetime.strptime("2022/05/15", "%Y/%m/%d")

    # MongoDB Connection
    config = get_json_config('../config.json')
    db = connection_mongo(config['mongo_db'])

    logs = db['ixon_logs']
    devices = db['ixon_devices']
    network_usage = db['network_usage']

    # Get buildings
    buildings = get_buildings()

    caption = f"# Informe Setmanal \nData Inci: {date_init}\nData Fi: {date_end.replace(hour=23, minute=59, second=59)}"

    report = dp.Report(
        dp.Text(caption),
        dp.Text("## Indicadors comunicació TC_Sistema"),
        dp.Plot(generate_request_per_building(date_init=date_init, date_end=date_end)),
        dp.Text("## Indicadors comunicació TC_Dispositiu"),
        dp.Plot(generate_request_per_devices(date_init=date_init, date_end=date_end)),
        dp.Text("## Indicadors tràfic dades"),
        dp.Plot(generate_network_usage(date_init=date_init, date_end=date_end)),
        dp.Plot(generate_network_usage_total_per_day(date_init=date_init, date_end=date_end)),
        dp.Text("## Indicadors tràfic dades per dispositiu"),
        *generate_network_usage_per_device(date_init=date_init, date_end=date_end)
    )

    report_name = f'informe_{date_end.date()}_{date_end.date()}'
    # report.save(path=report_name + '.html')
    report.upload(report_name, publicly_visible=True)
    link = report.url.replace("api/", "") + report_name
