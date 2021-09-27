import json
import pandas as pd
import os
import sys
sys.path.append(os.getcwd())
from utils import *


def get_config():
    f = open("Genercat/config.json", "r")
    return json.load(f)


def get_codeEns(text):
    if isinstance(text, str):
        if text.find("-") > 0:
            pos = text.find("-")
        elif text.find("_") > 0:
            pos = text.find("_")
        elif text.find("/"):
            pos = text.find("-")
        else:
            pos = -1
    else:
        pos = -1

    if pos > 0:
        return text[pos:].strip()
    else:
        return None


def isNaN(num):
    try:
        if float('-inf') < float(num) < float('inf'):
            return False
        else:
            return True
    except Exception:
        return False


def get_data():
    df = pd.read_excel('Genercat/data/genercat.xls')
    columns = list(df.columns)
    df["codeEns"] = df["building_CodeEns_GPG"].apply(get_codeEns)

    data = []
    for i in range(0, len(df)):
        item = {}
        for pos, column_name in list(enumerate(columns)):
            if not isNaN(df.iloc[i, pos]):
                if column_name == "improvement_percentage":
                    item[column_name] = str(df.iloc[i, pos])
                else:
                    item[column_name] = df.iloc[i, pos]
        data.append(item)
    return data


def load_genercat_hbase():
    config = get_config()
    hbase = connection_hbase(config["hbase"])
    HTable = 'genercat'
    htable = get_HTable(hbase, HTable, {"info": {}})
    documents = get_data()
    save_to_hbase(htable, documents, [("info", "all")], row_fields=None)


if __name__ == "__main__":
    load_genercat_hbase()
