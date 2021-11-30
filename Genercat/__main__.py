import argparse
import json
from Genercat.genercat_gather import get_data
from utils import *

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("-u", "--user", required=True, help="Excel file path to parse")
    # args_t = ["-f", "Genercat/data/genercat.xls", "-u", "eloi"]
    args = vars(ap.parse_args())
    with open("./config.json") as config_f:
        config = json.load(config_f)
    data = get_data(args['file'])
    h_table_name = f'genercat_eem_{args["user"]}'
    hbase = connection_hbase(config['hbase'])
    h_table = get_HTable(hbase, h_table_name, {"info": {}})
    save_to_hbase(h_table, data, [("info", "all")])
