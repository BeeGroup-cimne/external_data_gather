from utils import connection_mongo
from utils import connection_hbase, get_HTable, save_to_hbase


class DataSource:
    def __init__(self, config, name):
        self.name = name
        self.mongo_db = config["mongo_db"]
        self.hbase = config["hbase"]
        self.info = config["datasources"][name]["info"]
        self.hbase_name = config["datasources"][name]["hbase_name"]

    def get_metadata(self):
        mongo = connection_mongo(self.mongo_db)
        info = mongo[self.info].find_one({})
        if info is None:
            info = {"version": 0, "user": self.mongo_db["user"]}
        version = info["version"] + 1
        user = info["user"]
        return {"version": version, "user": user}

    def save(self, df, metadata):
        hbase = connection_hbase(self.hbase)
        htable = get_HTable(
            hbase,
            "{}_{}_{}".format(self.hbase_name, self.hbase["db"], metadata["user"]),
            {"info": {}},
        )
        records = df.to_dict("records")
        save_to_hbase(
            htable,
            records,
            [("info", "all")],
            row_fields=["num_cas"],
            version=metadata["version"],
        )
