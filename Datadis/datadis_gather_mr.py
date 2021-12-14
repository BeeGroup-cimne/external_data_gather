import glob
import pickle

from mrjob.job import MRJob
from utils import connection_hbase, save_to_hbase
from neo4j import GraphDatabase
from beedis import ENDPOINTS, datadis


class DatadisMRJob(MRJob):

    def mapper(self, key, value):
        driver = GraphDatabase.driver(self.neo4j['uri'], auth=(self.neo4j['user'], self.neo4j['password']))
        with driver.session() as session:
            session.run()

        yield key, value

    def reducer(self, key, values):
        pass

    def mapper_init(self):
        fn = glob.glob('*.pickle')
        config = pickle.load(open(fn[0], 'rb'))

        self.hbase = config['hbase']
        self.datasources = config['datasources']
        self.neo4j = config['neo4j']

    def reducer_init(self):
        pass


if __name__ == '__main__':
    DatadisMRJob.run()
