import json

from beedis import datadis, ENDPOINTS

from utils import connection_hbase, save_to_hbase, get_HTable


def get_config(path):
    f = open(path, "r")
    return json.load(f)


if __name__ == '__main__':
    # TODO add ParseArguments
    # TODO: Get Users from Neo4J

    config = get_config("config.json")

    for user in config['users']:
        try:
            datadis.connection(user['username'], user['password'], timezone="UTC")
            supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)

            for sup in supplies:
                sup['nif'] = user['username']

            hbase_connection = connection_hbase(config['hbase'])
            hTable = get_HTable(hbase_connection,
                                "{}_{}_{}".format(config['datasources']['datadis']['hbase_name'], 'supplies',
                                                  user['organisation']),
                                {"info": {}})

            save_to_hbase(hTable, supplies,
                          [("info",
                            "all")],
                          row_fields=['cups'])

            # TODO: contracts
            # contracts = datadis.datadis_query(ENDPOINTS.GET_CONTRACT)

        except Exception as ex:
            print(str(ex))
