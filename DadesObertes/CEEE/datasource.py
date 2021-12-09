from DadesObertes.datasource import DataSource
from DadesObertes.CEEE.client import CEEE

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CEEEDataSource(DataSource):
    def __init__(self, config):
        self.name = "CEEE"
        self.limit = config["datasources"][self.name]["limit"]
        super(CEEEDataSource, self).__init__(config, self.name)

    def gather(self):
        offset = 0
        metadata = self.get_metadata()
        while True:
            df = CEEE().query(limit=self.limit, offset=offset)
            if df.empty:
                break
            self.save(df, metadata)
            offset += self.limit
            logger.log(logging.INFO, "Saved {} registers from CEEE".format(offset))
