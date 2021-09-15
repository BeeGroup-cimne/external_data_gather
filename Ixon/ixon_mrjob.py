import json
import subprocess
import sys
import time

import BAC0
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
from pymongo import MongoClient

from Ixon import Ixon

NUM_VPNS_CONFIG = 1


class MRIxonJob(MRJob):
    def mapper_get_available_agents(self, _, line):
        # line : email password application_id
        l = line.split('\t')

        ixon_conn = Ixon(l[2])
        ixon_conn.generate_token(l[0], l[1])
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for index, agent in enumerate(ixon_conn.agents):
            dict_result = ixon_conn.get_credentials()
            dict_result.update({"agent": agent['publicId']})
            yield index % NUM_VPNS_CONFIG, dict_result

    def mapper_generate_network_config(self, key, line):

        try:
            # Request: get network configuration
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

            # Store data
            data = res.json()['data']

            if data is not None and data['devices']:
                ips = {}

                ips['ip_vpn'] = data['activeVpnSession']['vpnAddress'] if data['activeVpnSession'] else None

                if data['config'] is not None and data['config']['routerLan'] is not None:
                    ips['network'] = data['config']['routerLan']['network']
                    ips['network_mask'] = data['config']['routerLan']['netMask']

                if data['deviceId'] is not None:
                    ips['deviceId'] = data['deviceId']
                    ips['bacnet_device'] = None

                    for i in data['devices']:
                        if i['dataProtocol'] is not None and i['dataProtocol']['publicId'] == 'bacnet-ip':
                            ips['bacnet_device'] = i['ipAddress']
                            break

                # Only return data that use bacnet protocol
                if ips['bacnet_device'] is not None and ips['network'] is not None and ips[
                    'network_mask'] is not None and ips['ip_vpn'] is not None:
                    yield key, ips

        except Exception as ex:
            print(ex)

    def reducer_generate_vpn(self, key, values):
        for value in values:
            # Generate VPN Config
            with open(f'vpn_template_{key}.ovpn', 'r') as file:
                f = open(value['deviceId'].split('-')[1].strip() + '.ovpn', 'w')
                content = file.read()
                content += "\nroute {0} {1} {2}".format(value['network'], value['network_mask'], value['ip_vpn'])
                # content += "\nroute 10.81.182.0 255.255.255.0 10.187.113.206"
                f.write(content)
                f.close()

            # Connect to VPN
            openvpn = subprocess.Popen(["sudo", "openvpn", f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
            interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")

            # Service loop
            time_out = 3  # seconds
            init_time = time.time()
            waiting_time = 0.2

            while "10.187.10.1" not in interfaces_list:
                time.sleep(waiting_time)
                interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")

                if time.time() - init_time > time_out:
                    raise Exception("VPN Connection: Time out exceded.")

            # Read devices from mongo
            URI = 'mongodb://%s:%s@%s:%s/%s' % (
                self.connection['user'], self.connection['password'], self.connection['host'],
                self.connection['port'],
                self.connection['db'])

            mongo_connection = MongoClient(URI)
            db = mongo_connection[self.connection['db']]
            collection = db['ixon_devices']

            building_devices = list(collection.find({'building_id': key}, {'_id': 0}))

            out = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
            sys.stderr.write(out.stdout.decode(encoding="utf-8"))
            sys.stderr.write(value['deviceId'] + "\n")

            # Recover Data
            bacnet = BAC0.lite(ip='10.187.10.1/16', bbmdAddress=value['bacnet_device'] + ':47808', bbmdTTL=9000)

            x = [bacnet.read(f"{value['bacnet_device']} {value['type']} {value['object_id']} presentValue") for
                 devices
                 in building_devices]
            # regex:  /(^|\s)10.187/g

            # Save data to HBase

            # Stop Openvpn
            bacnet.disconnect()
            subprocess.call(["sudo", "pkill", "openvpn"])
            out = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
            sys.stderr.write(out.stdout.decode(encoding="utf-8"))
            yield key, str(x)

    def reducer_init_mongo(self):
        # Read and save MongoDB config
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
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py --file vpn_template_0.ovpn --file config.json
    # docker run --cap-add=NET_ADMIN --device=/dev/net/tun -it -v /home/ubuntu/ixon_test:/home/ubuntu/ixon_test -u root beerepo.tech.beegroup-cimne.com:5000/ixon_mr bash
    MRIxonJob.run()
