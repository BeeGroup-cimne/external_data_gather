import argparse
import pandas as pd
import json
import sys
import os
from datetime import datetime
sys.path.append(os.getcwd())
from utils import *


# data clean functions
def get_municipi(data):
    return data.split("(")[0].strip()


def get_codi_postal(data):
    if len(data.split("(")) > 1:
        cod_p = data.split("(")[1]
        result = cod_p[:-1]
    else:
        result = None
    return result


def remove_semicolon(data):
    result = None
    if data:
        result = str(data)[:-1]
    return result            


# Main gather function
def save_data_from_xlsx(file):
    columns = ["Num_Ens_Inventari", "Provincia", "Municipi", "Via", "Num_via",
               "Departament_Assig_Adscrip", "Espai", "Tipus_us",
               "Sup_const_sobre_rasant", "Sup_const_sota rasant",
               "Sup_const_total", "Sup_terreny", "Classificacio_sol",
               "Qualificacio_urbanistica", "Clau_qualificacio_urbanistica",
               "Ref_Cadastral"]
    df = pd.read_excel(file, sheet_name='SGT_Informe General Immobl_0', names=columns, skiprows=list(range(1, 9)))
    df["Codi_postal"] = df["Municipi"].apply(get_codi_postal)
    df["Municipi"] = df["Municipi"].apply(get_municipi)
    df["Departament_Assig_Adscrip"] = df["Departament_Assig_Adscrip"].apply(remove_semicolon)
    df["Classificacio_sol"] = df["Classificacio_sol"].apply(remove_semicolon)
    df["Ref_Cadastral"] = df["Ref_Cadastral"].apply(remove_semicolon)
    df.reset_index(inplace=False)
    return df.to_dict("records")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    args = vars(ap.parse_args())
    with open("./config.json") as config_f:
        config = json.load(config_f)
    data_source = config['datasources']['GPG']

    # get mongo info:
    mongo = connection_mongo(config['mongo_db'])
    gpg_info = mongo[data_source['info']].find_one({})
    version = gpg_info['version'] + 1
    user = gpg_info['user']

    # connect to hbase
    hbase = connection_hbase(config['hbase'])
    htable_buildings = get_HTable(hbase, "{}_{}_{}".format(data_source["hbase_name"], "buildings", user), {"info": {}})

    # read file and save to mongo
    gpg_list = save_data_from_xlsx(file=args['file'])
    save_to_hbase(htable_buildings, gpg_list, [("info", "all")], row_fields=['Num_Ens_Inventari'], version=version)

    # update info
    gpg_info['version'] = version
    gpg_info['inserted'] = len(gpg_list)
    gpg_info['date'] = datetime.now()
    mongo[data_source['info']].replace_one({"_id": gpg_info["_id"]}, gpg_info)
