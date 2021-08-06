import requests
from mrjob.job import MRJob
from mrjob.step import MRStep
import subprocess
import Ixon


class MRIxonJob(MRJob):
    def mapper_get_credentials(self, _, line):
        l = line.split('\t')
        ixon_conn = Ixon.Ixon(l[2])
        ixon_conn.generate_token(l[0], l[1])
        ixon_conn.discovery()
        ixon_conn.get_companies()
        ixon_conn.get_agents()

        for agent in ixon_conn.agents:
            yield agent['publicId'], ixon_conn.get_all_credentials()

    def mapper_get_agent_ip(self, key, line):

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

    def reducer_ips(self, key, values):
        # Generate VPN File

        subprocess.call('hdfs dfs -cp -f /vpn_template.ovpn /vpn_%s.ovpn' % key, shell=True)

        # Connect to VPN

        # Read devices from hdfs/mongo

        # Recover Data

        # Save data to HBase
        yield key, None

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_credentials),
            MRStep(mapper=self.mapper_get_agent_ip, reducer=self.reducer_ips)
        ]


if __name__ == '__main__':
    # python ixon_mrjob.py -r hadoop hdfs:///output.tsv --file Ixon.py
    MRIxonJob.run()
