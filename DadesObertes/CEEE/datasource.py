from DadesObertes.datasource import DataSource
from DadesObertes.CEEE.client import CEEE


class CEEEDataSource(DataSource):
    def __init__(self, config):
        self.name = "CEEE"
        self.limit = config["datasources"][self.name]["limit"]
        super(CEEEDataSource, self).__init__(config, self.name)

    def gather(self):
        offset = 0
        while True:
            df = CEEE().query(limit=self.limit, offset=offset)
            if df.empty:
                break
            metadata = self.get_metadata()
            self.save(df, metadata)
            offset += self.limit
