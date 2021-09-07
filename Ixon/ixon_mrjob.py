import glob
import pickle
from tempfile import NamedTemporaryFile

import requests
from mrjob.job import MRJob
from mrjob.step import MRStep

from Ixon import Ixon


class MRIxonJob(MRJob):
    def mapper_get_available_agents(self, _, line):
        l = line.split('\t')
        ixon_conn = Ixon(l[2])
        ixon_conn.generate_token(l[0], l[1])
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for agent in ixon_conn.agents:
            yield agent['publicId'], ixon_conn.get_credentials()

    def mapper_generate_network_config(self, key, line):

        try:
            res = requests.get(
                line[
                    'url'] + '/agents/{}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId'.format(
                    key),
                headers={
                    'IXapi-Version': line['api_version'],
                    'IXapi-Application': line['api_application'],
                    'Authorization': line['token'],
                    'IXapi-Company': line['company']
                }, timeout=20
            )

            data = res.json()['data']

            if data is not None and data['devices']:
                ips = {}

                if data['activeVpnSession']:
                    ips['ip_vpn'] = data['activeVpnSession']['vpnAddress']
                else:
                    ips['ip_vpn'] = None

                if data['config'] is not None:
                    if data['config']['routerLan'] is not None:
                        ips['network'] = data['config']['routerLan']['network']
                        ips['network_mask'] = data['config']['routerLan']['netMask']

                if data['deviceId'] is not None:
                    ips['deviceId'] = data['deviceId']
                    ips['bacnet_device'] = None

                    for i in data['devices']:
                        if i['dataProtocol'] is not None and i['dataProtocol']['publicId'] == 'bacnet-ip':
                            ips['bacnet_device'] = i['ipAddress']
                            break

                if ips['bacnet_device'] is not None:
                    yield ips['deviceId'], ips

        except Exception as ex:
            print(ex)

    def mapper_generate_vpn(self, key, values):

        # Generate VPN Config
        with open('vpn_template.ovpn', 'r') as file:
            f = NamedTemporaryFile(suffix='.ovpn')
            content = file.read()

            content += "\nroute {0} {1} {2}".format(values['network'], values['network_mask'], values['ip_vpn'])
            f.write(bytes(content, 'utf-8'))
            f.close()
            yield key, content

        # Connect to VPN

        # Read devices from hdfs/mongo

        # Recover Data

        # Save data to HBase

    def mapper_init_mongo(self):
        fn = glob.glob('*.json')
        config = pickle.load(open(fn[0], 'rb'))
        self.connection = config['mongo_db']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper=self.mapper_generate_network_config),
            MRStep(mapper_init=self.mapper_init_mongo, mapper=self.mapper_generate_vpn)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py --file vpn_template.ovpn
    MRIxonJob.run()
