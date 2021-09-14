import json
import subprocess
import sys
from tempfile import NamedTemporaryFile

import BAC0
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
from pymongo import MongoClient
import time
from Ixon import Ixon


class MRIxonJob(MRJob):
    def mapper_get_available_agents(self, _, line):
        num_vpns = 1
        l = line.split('\t')
        ixon_conn = Ixon(l[2])
        ixon_conn.generate_token(l[0], l[1])
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()
        # TODO: Bucle 0 a N (N = len VPN), yield n, (agent+credencials)
        for index, agent in enumerate(ixon_conn.agents):
            dict_result = ixon_conn.get_credentials()
            dict_result.update({"agent": agent['publicId']})
            yield index % num_vpns, dict_result

    def mapper_generate_network_config(self, key, line):

        try:
            res = requests.get(
                line[
                    'url'] + '/agents/{}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId'.format(
                    line['agent']),
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
                    yield key, ips

        except Exception as ex:
            print(ex)

    def reducer_generate_vpn(self, key, values):
        for value in values:
            # values = values['deviceId'] + '.ovpn',
            #     Generate VPN Config
            with open(f'vpn_template_{key}.ovpn', 'r') as file:
                # f = open(key.split('-')[1].strip() + '.ovpn', 'w')
                f = open('test.ovpn', 'w')
                content = file.read()
                content += "\nroute {0} {1} {2}".format(value['network'], value['network_mask'], value['ip_vpn'])
                # content += "\nroute 10.81.182.0 255.255.255.0 10.187.113.206"
                f.write(content)
                f.close()

            # Connect to VPN
            # subprocess.Popen("echo <password> | sudo -S openvpn --config Ixon/vpn_template.ovpn", shell=True)

            openvpn = subprocess.Popen(["sudo", "openvpn", f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
            interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")
            while "10.187.10.1" not in interfaces_list:
                time.sleep(0.2)
                interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")

            #
            # # Read devices from mongo
            # URI = 'mongodb://%s:%s@%s:%s/%s' % (
            #     self.connection['user'], self.connection['password'], self.connection['host'], self.connection['port'],
            #     self.connection['db'])
            #
            # mongo_connection = MongoClient(URI)
            # db = mongo_connection[self.connection['db']]
            # collection = db['ixon_devices']
            #
            # building_devices = list(collection.find({'building_id': key}, {'_id': 0}))
            #
            # # Recover Data
            bacnet = BAC0.lite(ip='10.187.10.1/16', bbmdAddress='10.187.113.206:47808', bbmdTTL=9000)
            val = bacnet.read("10.81.182.10 analogInput 1 presentValue")
            # val.stdout.decode(encoding="utf-8").split(" ")
            # x = [bacnet.read(f"10.81.182.10 {device['type']} {device['object_id']} presentValue") for device in
            #      building_devices]
            # # Save data to HBase
            out = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
            sys.stderr.write(out.stdout.decode(encoding="utf-8"))
            subprocess.call(["sudo", "pkill", "openvpn"])
            yield key, val
        else:
            yield key, "notok"

    def reducer_init_mongo(self):
        with open('config.json', 'r') as file:
            config = json.load(file)
        self.connection = config['mongo_db']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper=self.mapper_generate_network_config),
            MRStep(reducer_init=self.reducer_init_mongo, reducer=self.reducer_generate_vpn)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py --file vpn_template.ovpn --file config.json
    # docker run --cap-add=NET_ADMIN --device=/dev/net/tun -it -v /home/ubuntu/ixon_test:/home/ubuntu/ixon_test -u root beerepo.tech.beegroup-cimne.com:5000/ixon_mr bash
    MRIxonJob.run()
