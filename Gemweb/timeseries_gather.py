from pyhive import hive

# create supplies hdfs file to perform mapreduce
create_table_hbase = """CREATE EXTERNAL TABLE supplies_aux(id string, value string) 
                        STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
                        WITH SERDEPROPERTIES (
                            'hbase.table.name' = 'raw_data:gemweb_supplies_icaen', 
                            "hbase.columns.mapping" = ":key,info:cups"
                        )"""

save_id_to_file = """INSERT OVERWRITE DIRECTORY '/tmp/supplies_aux/' SELECT id FROM supplies_aux"""

cursor = hive.Connection("master1.internal", 10000, database="gemweb").cursor()

cursor.execute(create_table_hbase)
cursor.execute(save_id_to_file)


