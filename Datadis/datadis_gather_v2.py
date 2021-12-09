from pyhive import hive

if __name__ == '__main__':
    hbase_table = "raw_data:datadis_supplies_icaen"
    hdfs_file = f"datadis_supplies_icaen"

    create_table_hbase = f"""CREATE EXTERNAL TABLE {hdfs_file}(id string, value string)
                            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                            WITH SERDEPROPERTIES (
                                'hbase.table.name' = '{hbase_table}', "hbase.columns.mapping" = ":key,info:address,
                                info:postalCode,info:province,info:municipality,info:distributor,info:validDateFrom,
                                info:validDateTo,info:pointType,info:distributorCode" ) """

    save_id_to_file = f"""INSERT OVERWRITE DIRECTORY '/tmp/{hdfs_file}/' SELECT id FROM {hdfs_file}"""

    cursor = hive.Connection("master1.internal", 10000, database="gemweb").cursor()
    cursor.execute(create_table_hbase)
    cursor.execute(save_id_to_file)
    # cursor.execute(remove_hbase_table)
    cursor.close()
