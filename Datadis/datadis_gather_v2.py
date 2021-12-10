import json

from pyhive import hive


def get_config(path):
    f = open(path, "r")
    return json.load(f)


if __name__ == '__main__':
    config = get_config('config.json')

    hbase_table = "raw_data:datadis_supplies_icaen"
    hdfs_file = "datadis_supplies_icaen"

    create_table_hbase = f"""CREATE EXTERNAL TABLE {hdfs_file} (id string, address string, postalCode string, province string, municipality string, distributor string, validDateFrom string, validDateTo string, pointType string, distributorCode string)
                            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                            WITH SERDEPROPERTIES (
                                'hbase.table.name' = '{hbase_table}', "hbase.columns.mapping" = ":key,info:address,info:postalCode,info:province,info:municipality,info:distributor,info:validDateFrom,info:validDateTo,info:pointType,info:distributorCode")"""

    save_id_to_file = f"""INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' SELECT * FROM {hdfs_file}"""

    remove_hbase_table = f"""DROP TABLE {hdfs_file}"""

    cursor = hive.Connection("master1.internal", 10000, database='bigg').cursor()

    cursor.execute(create_table_hbase)
    cursor.execute(save_id_to_file)
    cursor.execute(remove_hbase_table)
    cursor.close()

    # TODO: map reduce
