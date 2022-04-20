import datetime
import sys

import BAC0
import requests
from mrjob.job import MRJob
from mrjob.step import MRStep

from Ixon import Ixon
from utils import *

import psutil

NUM_VPNS_CONFIG = 4
NETWORK_INTERFACE = 'tap0'

vpn_dict = {'0': '10.187.10.1', '1': '10.187.10.15', '2': '10.187.10.12', '3': '10.187.10.13', '4': '10.187.10.14'}


class MRIxonJob(MRJob):
    def mapper_get_available_agents(self, _, line):
        # line : email password application_id
        l = line.split('\t')

        ixon_conn = Ixon(l[2])  # api application
        ixon_conn.generate_token(l[0], l[1])  # user, password
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for index, agent in enumerate(ixon_conn.agents):
            dict_result = {"token": ixon_conn.token, "api_application": l[2], "company": ixon_conn.companies[0]}
            dict_result.update({"agent": agent['publicId']})
            dict_result.update({"company_label": l[3]})
            yield index % NUM_VPNS_CONFIG, dict_result

    def mapper_generate_network_config(self, key, line):

        try:
            headers = {
                "Api-Version": '2',
                "Api-Application": line['api_application'],
                'Authorization': f"Bearer {line['token']}",
                "Api-Company": line['company']['publicId']
            }

            # Get network configuration
            res = requests.get(
                url=f"https://portal.ixon.cloud/api/agents/{line['agent']}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId,description",
                headers=headers,
                timeout=(5, 10))

            # Store data
            res = res.json()

            if 'data' in res:
                data = res['data']

                if 'deviceId' in data:
                    db = connection_mongo(self.connection)
                    ixon_devices = db['ixon_devices']
                    current_buildings = list(ixon_devices.distinct('building_id'))

                    if data['deviceId'] in current_buildings:
                        if data['activeVpnSession'] and data['config'] and data['config']['routerLan']:
                            values = {"company_label": line['company_label'],
                                      'ip_vpn': data['activeVpnSession']['vpnAddress'],
                                      'network': data['config']['routerLan']['network'],
                                      'network_mask': data['config']['routerLan']['netMask'],
                                      'deviceId': data['deviceId'],
                                      'description': data['description']}
                            yield key, values

        except Exception as ex:
            print(str(ex))

    def reducer_generate_vpn(self, key, values):

        db = connection_mongo(self.connection)

        ixon_devices = db['ixon_devices']
        ixon_logs = db['ixon_logs']
        network_usage = db['network_usage']

        for value in values:  # buildings

            building_devices = list(ixon_devices.find({'building_id': value['deviceId']}, {'_id': 0}))

            if building_devices:
                sys.stderr.write(f"{building_devices[0]['building_name']}\n")

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

                sys.stderr.write(f"{interfaces_list}\n")

                # Waiting to VPN connection
                time_out = 4  # seconds
                init_time = time.time()
                waiting_time = 0.2

                connected = vpn_dict[str(key)] in interfaces_list

                while not connected:
                    time.sleep(waiting_time)
                    interfaces = subprocess.run(["hostname", "-I"], stdout=subprocess.PIPE)
                    interfaces_list = interfaces.stdout.decode(encoding="utf-8").split(" ")
                    connected = vpn_dict[str(key)] in interfaces_list
                    if time.time() - init_time > time_out:
                        # TODO: Alarma connexio perduda.
                        raise Exception("VPN Connection: Time out exceded.")

                vpn_ip = vpn_dict[str(key)]
                sys.stderr.write(str(vpn_ip))

                # Recover Data
                # Open BACnet Connection
                results = []
                devices_logs = []
                current_time = time.time()
                aux = False

                while not aux and time.time() - current_time < 3:
                    try:
                        sys.stderr.write(f"{vpn_ip},{value['ip_vpn']}\n")
                        bacnet = BAC0.lite(ip=vpn_ip + '/16', bbmdAddress=value['ip_vpn'] + ':47808', bbmdTTL=900)
                        aux = True
                    except Exception as ex:
                        bacnet.disconnect()
                        sys.stderr.write(str(ex))
                        sys.stderr.write("%s " % str(building_devices[0]['building_name']) + "\n")
                        time.sleep(0.2)

                if not aux:

                    try:
                        netio = psutil.net_io_counters(pernic=True)
                        network_usage.insert_one(
                            {"from": 'infraestructures.cat', "building": value['deviceId'],
                             "bytes_sent": netio[NETWORK_INTERFACE].bytes_sent,
                             "bytes_recv": netio[NETWORK_INTERFACE].bytes_recv})
                    except Exception as ex:
                        sys.stderr.write(str(ex))

                    subprocess.call(["sudo", "pkill", "openvpn"])
                    bacnet.disconnect()
                    ixon_logs.insert_one(
                        {'building_id': value['deviceId'], "building_name": building_devices[0]['building_name'],
                         "devices_logs": devices_logs,
                         "date": datetime.datetime.utcnow(), "successful": False})
                    continue

                # Recover data for each device
                for device in building_devices:
                    try:
                        device_value = bacnet.read(
                            f"{device['bacnet_device_ip']} {device['type']} {device['object_id']} presentValue")

                        # TODO: save value['description']

                        results.append({"building": device['building_id'], "device": device['name'],
                                        "timestamp": datetime.datetime.utcnow().timestamp(), "value": device_value,
                                        "type": device['type'], "description": device['description'],
                                        "object_id": device['object_id']})

                        devices_logs.append({'device_name': device['name'],
                                             'device_id': device['object_id'], 'device_type': device['type'],
                                             'successful': True})
                    except Exception as ex:
                        devices_logs.append({'device_name': device['name'],
                                             'device_id': device['object_id'], 'device_type': device['type'],
                                             'successful': False})

                try:
                    netio = psutil.net_io_counters(pernic=True)
                    network_usage.insert_one(
                        {"from": 'infraestructures.cat', "building": value['deviceId'],
                         "bytes_sent": netio[NETWORK_INTERFACE].bytes_sent,
                         "bytes_recv": netio[NETWORK_INTERFACE].bytes_recv})
                except Exception as ex:
                    sys.stderr.write(str(ex))

                # End Connections (Bacnet and VPN)
                bacnet.disconnect()
                subprocess.call(["sudo", "pkill", "openvpn"])

                ixon_logs.insert_one(
                    {'building_id': value['deviceId'], "building_name": building_devices[0]['building_name'],
                     "devices_logs": devices_logs,
                     "date": datetime.datetime.utcnow(), "successful": True})

                if results:
                    # Store data to HBase
                    try:
                        hbase = connection_hbase(self.hbase)
                        htable = get_HTable(hbase,
                                            "{}_{}_{}".format(self.datasources['ixon']["hbase_name"], "data",
                                                              value['company_label']),
                                            {"v": {}, "info": {}})

                        save_to_hbase(htable, results,
                                      [("v", ["value"]), ("info", ["type", "description", 'object_id'])],
                                      row_fields=['building', 'device', 'timestamp'])
                    except Exception as ex:
                        sys.stderr.write(str(ex))

    def reducer_init_databases(self):
        # Read and save MongoDB config
        config = get_json_config('config.json')
        self.connection = config['mongo_db']
        self.hbase = config['hbase']
        self.datasources = config['datasources']

    def mapper_init(self):
        # Read and save MongoDB config
        config = get_json_config('config.json')
        self.connection = config['mongo_db']

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_available_agents),
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper_generate_network_config),
            MRStep(reducer_init=self.reducer_init_databases, reducer=self.reducer_generate_vpn)]


if __name__ == '__main__':
    MRIxonJob.run()
