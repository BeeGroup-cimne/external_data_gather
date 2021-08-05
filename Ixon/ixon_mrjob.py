from mrjob.job import MRJob
from mrjob.step import MRStep

import Ixon

ixon_conn = None


class MRIxonJob(MRJob):
    def mapper_get_agents(self, _, line):
        l = line.split('\t')
        ixon_conn = Ixon.Ixon(l[2])
        ixon_conn.generate_token(l[0], l[1])
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        yield None, ixon_conn.agents

    def mapper_get_ip(self, _, line):
        yield line['publicId'], ixon_conn.get_agent_ips(agent['publicId'])

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_agents),
            MRStep(mapper=self.mapper_get_ip)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py
    MRIxonJob.run()
