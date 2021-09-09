import json
import subprocess
from tempfile import NamedTemporaryFile

import BAC0
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
from pymongo import MongoClient

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
            f = open(key + '.ovpn', 'w')
            content = file.read()
            content += "\nroute {0} {1} {2}".format(values['network'], values['network_mask'], values['ip_vpn'])
            f.write(content)

            # Connect to VPN
            # subprocess.Popen("echo <password> | sudo -S openvpn --config Ixon/vpn_template.ovpn", shell=True)
            subprocess.Popen("openvpn --config %s" % f.name, shell=True)

            # Read devices from mongo
            URI = 'mongodb://%s:%s@%s:%s/%s' % (
                self.connection['user'], self.connection['password'], self.connection['host'], self.connection['port'],
                self.connection['db'])

            mongo_connection = MongoClient(URI)
            db = mongo_connection[self.connection['db']]
            collection = db['ixon_devices']

            building_devices = list(collection.find({'building_id': key}, {'_id': 0}))

            # # Recover Data
            # bacnet = BAC0.lite(ip='10.187.10.1/16', bbmdAddress=values['network'] + ':47808', bbmdTTL=9000)

            # for device in building_devices:
            #     val = bacnet.read(f"{values['bacnet_device']} {device['type']} {device['object_id']} presentValue")
            #     # Save data to HBase
            # subprocess.Popen("/usr/bin/killall openvpn", shell=True)
            f.close()
            yield key, f.name

    def mapper_init_mongo(self):
        with open('config.json', 'r') as file:
            config = json.load(file)
        self.connection = config['mongo_db']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper=self.mapper_generate_network_config),
            MRStep(mapper_init=self.mapper_init_mongo, mapper=self.mapper_generate_vpn)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py --file vpn_template.ovpn --file config.json
    # docker run --cap-add=NET_ADMIN --device=/dev/net/tun -it -v /home/ubuntu/ixon_test:/home/ubuntu/ixon_test -u root beerepo.tech.beegroup-cimne.com:5000/ixon_mr bash
    MRIxonJob.run()
