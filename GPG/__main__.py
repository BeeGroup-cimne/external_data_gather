import argparse
import json
from GPG.GPG_gather import read_data_from_xlsx
from utils import connection_hbase, get_HTable, save_to_hbase

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-f", "--file", required=True, help="Excel file path to parse")
    ap.add_argument("-u", "--user", required=True, help="Excel file path to parse")
    args_t = ["-f", "GPG/data/Llistat immobles alta inventari (13-04-2021).xlsx", "-u", "eloi"]

    args = vars(ap.parse_args(args_t))
    with open("./config.json") as config_f:
        config = json.load(config_f)
    gpg_list = read_data_from_xlsx(file=args['file'])

    h_table_name = f"GPG_buildings_{args['user']}"

    hbase = connection_hbase(config['hbase'])
    h_table_buildings = get_HTable(hbase, h_table_name, {"info": {}})
    save_to_hbase(h_table_buildings, gpg_list, [("info", "all")], row_fields=['Num_Ens_Inventari'])
