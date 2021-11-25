import datetime
import json
import re
import subprocess
import sys

import BAC0
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep

from Ixon import Ixon
from utils import *

NUM_VPNS_CONFIG = 5
vpn_network_ip = "(^| )10\.187"


class MRIxonJob(MRJob):
    def mapper_get_available_agents(self, _, line):
        # line : email password application_id
        l = line.split('\t')

        ixon_conn = Ixon(l[2])  # api application
        ixon_conn.generate_token(l[0], l[1])  # user, password
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for index, agent in enumerate(ixon_conn.agents):
            dict_result = ixon_conn.get_credentials()
            dict_result.update({"agent": agent['publicId']})
            dict_result.update({"company_label": l[3]})
            yield index % NUM_VPNS_CONFIG, dict_result

    def mapper_generate_network_config(self, key, line):

        try:
            # Get network configuration
            res = requests.get(
                line[
                    'url'] + '/agents/{}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId,description'.format(
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
                ips["company_label"] = line['company_label']

                ips['ip_vpn'] = data['activeVpnSession']['vpnAddress'] if data['activeVpnSession'] else None
                ips['description'] = data['description']

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

        # Recover devices from mongo
        URI = 'mongodb://%s:%s@%s:%s/%s' % (
            self.connection['user'], self.connection['password'], self.connection['host'],
            self.connection['port'],
            self.connection['db'])

        mongo_connection = MongoClient(URI)
        db = mongo_connection[self.connection['db']]
        ixon_devices = db['ixon_devices']
        ixon_logs = db['ixon_logs']

        for value in values:  # buildings

            building_devices = list(ixon_devices.find({'building_id': value['deviceId']}, {'_id': 0}))

            if len(building_devices) > 0:
                # sys.stderr.write(str(building_devices[0]['building_name']) + "\n")

                # Generate VPN Config
                with open(f'vpn_template_{key}.ovpn', 'r') as file:
                    f = open(value['deviceId'].split('-')[1].strip() + '.ovpn', 'w')
                    content = file.read()
                    content += "\nroute {0} {1} {2}".format(value['network'], value['network_mask'], value['ip_vpn'])
                    f.write(content)
                    f.close()

                # Connect to VPN
                openvpn = subprocess.Popen(["sudo", "openvpn", f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")

                # Waiting to VPN connection
                time_out = 4  # seconds
                init_time = time.time()
                waiting_time = 0.2

                vpn_ip = [x for x in interfaces_list if re.match(vpn_network_ip, x)]

                while not vpn_ip:
                    time.sleep(waiting_time)
                    interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                    interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")
                    vpn_ip = [x for x in interfaces_list if re.match(vpn_network_ip, x)]
                    if time.time() - init_time > time_out:
                        # TODO: Alarma connexio perduda.
                        raise Exception("VPN Connection: Time out exceded.")

                # Recover Data
                # Open BACnet Connection
                results = []
                logs = []

                bacnet = BAC0.lite(ip=vpn_ip[0] + '/16', bbmdAddress=value['ip_vpn'] + ':47808', bbmdTTL=900)

                # Recover data for each device
                for device in building_devices:
                    try:
                        device_value = bacnet.read(
                            f"{device['bacnet_device_ip']} {device['type']} {device['object_id']} presentValue")

                        # TODO: save value['description']

                        results.append({"building": device['building_id'], "device": device['name'],
                                        "timestamp": datetime.datetime.now().timestamp(), "value": device_value,
                                        "type": device['type'], "description": device['description'],
                                        "object_id": device['object_id']})

                        logs.append({'building_id': device['building_id'], 'building_name': device['building_name'],
                                     'device_name': device['name'],
                                     'device_id': device['object_id'], 'device_type': device['type'],
                                     'successful': True,
                                     'date': datetime.datetime.utcnow()})

                    except Exception as ex:
                        logs.append(
                            {'building_id': device['building_id'], 'building_name': device['building_name'],
                             'device_name': device['name'],
                             'device_id': device['object_id'], 'device_type': device['type'], 'successful': False,
                             'date': datetime.datetime.utcnow()})

                # End Connections (Bacnet and VPN)
                bacnet.disconnect()
                subprocess.call(["sudo", "pkill", "openvpn"])

                if len(logs) > 0:
                    try:
                        ixon_logs.insert_many(logs)
                    except Exception as ex:
                        sys.stderr.write(str(ex))

                if len(results) > 0:
                    # Store data to HBase
                    try:
                        hbase = connection_hbase(self.hbase)
                        data_source = self.datasources['ixon']
                        htable = get_HTable(hbase,
                                            "{}_{}_{}".format(data_source["hbase_name"], "data",
                                                              value['company_label']),
                                            {"v": {}, "info": {}})

                        save_to_hbase(htable, results,
                                      [("v", ["value"]), ("info", ["type", "description", 'object_id'])],
                                      row_fields=['building', 'device', 'timestamp'])
                    except Exception as ex:
                        sys.stderr.write(str(ex))

                # out = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                # sys.stderr.write(out.stdout.decode(encoding="utf-8"))

                # yield key, str(results)

    def reducer_init_databases(self):
        # Read and save MongoDB config
        with open('config.json', 'r') as file:
            config = json.load(file)
        self.connection = config['mongo_db']
        self.hbase = config['hbase']
        self.datasources = config['datasources']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper=self.mapper_generate_network_config),
            MRStep(reducer_init=self.reducer_init_databases, reducer=self.reducer_generate_vpn)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py --file vpn_template_0.ovpn --file config.json
    # docker run --cap-add=NET_ADMIN --device=/dev/net/tun -it -v /home/ubuntu/ixon_test:/home/ubuntu/ixon_test -u root beerepo.tech.beegroup-cimne.com:5000/ixon_mr bash
    MRIxonJob.run()
