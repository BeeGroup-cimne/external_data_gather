import base64
import json
import requests

API_URL = 'https://api.ixon.net:443/'


class Ixon:
    def __init__(self, api_application):
        self.api_version = '1'
        self.api_url = API_URL
        self.api_application = api_application
        self.token = None
        self.token_authorization = None
        self.discovered = None
        self.companies = None
        self.agents = None
        self.agents_types = None
        self.agents_categories = None
        self.agents_configuration = None

    def generate_token(self, email, password):
        res = requests.post(url=self.api_url + '/access/tokens?fields=secretId', headers={
            'IXapi-Version': self.api_version,
            'IXapi-Application': self.api_application,
            'Authorization': 'Basic ' + base64.b64encode(bytes('%s::%s' % (email, password), 'utf-8')).decode(
                'utf-8')
        }, data={'expiresIn': 36000}, timeout=(5, 20))

        assert 201 == res.status_code
        self.token = res.json()['data']['secretId']
        self.token_authorization = 'Bearer %s' % res.json()['data']['secretId']

    def discovery(self):
        res = requests.get(
            self.api_url,
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
            }
        )
        assert 200 == res.status_code

        self.discovered = {
            row['rel']: row['href']
            for row in res.json()['links']
        }

    def get_companies(self):
        res = requests.get(
            self.discovered['CompanyList'],
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
            }
        )
        assert 200 == res.status_code

        self.companies = res.json()['data']

    def get_agents(self):
        # TODO: Loop companies
        res = requests.get(
            self.discovered['AgentList'],
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            }
        )
        assert 200 == res.status_code

        self.agents = res.json()['data']

    def get_agent(self, publicId):
        res = requests.get(self.discovered['Agent'].replace('{publicId}', publicId),
                           headers={
                               'IXapi-Version': self.api_version,
                               'IXapi-Application': self.api_application,
                               'Authorization': self.token_authorization,
                               'IXapi-Company': self.companies[0]['publicId']
                           }
                           )
        assert 200 == res.status_code

        return res.json()['data']

    def get_agent_ips(self, publicId):
        try:
            res = requests.get(
                self.api_url + '/agents/{}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId'.format(
                    publicId),
                headers={
                    'IXapi-Version': self.api_version,
                    'IXapi-Application': self.api_application,
                    'Authorization': self.token_authorization,
                    'IXapi-Company': self.companies[0]['publicId']
                }, timeout=20
            )

            data = res.json()['data']

            if data is not None and data['devices']:
                ips = {}
                ips['my_vpn_ip'] = '10.187.10.1/16'

                if data['activeVpnSession']:
                    ips['ip_vpn'] = data['activeVpnSession']['vpnAddress']
                else:
                    ips['ip_vpn'] = None

                if data['config'] is not None:
                    if data['config']['routerLan'] is not None:
                        ips['network'] = data['config']['routerLan']['network']
                        ips['network_mask'] = data['config']['routerLan']['netMask']
                        ips['network_mask_bits'] = self.network_mask_to_bits(data['config']['routerLan']['netMask'])

                if data['deviceId'] is not None:
                    ips['deviceId'] = data['deviceId']
                    ips['bacnet_device'] = None

                    for i in data['devices']:
                        if i['dataProtocol'] is not None and i['dataProtocol']['publicId'] == 'bacnet-ip':
                            ips['bacnet_device'] = i['ipAddress']
                            break

                if ips['bacnet_device'] is not None:
                    return ips

        except Exception as ex:
            print(ex)

    def get_agent_types(self):
        res = requests.get(self.discovered['AgentTypeList'],
                           headers={
                               'IXapi-Version': self.api_version,
                               'IXapi-Application': self.api_application,
                               'Authorization': self.token_authorization
                           })

        assert 200 == res.status_code
        self.agents_types = res.json()['data']

    def get_agent_categories(self):
        res = requests.get(self.discovered['AgentCategoryList'],
                           headers={
                               'IXapi-Version': self.api_version,
                               'IXapi-Application': self.api_application,
                               'Authorization': self.token_authorization,
                               'IXapi-Company': self.companies[0]['publicId']
                           })

        assert 200 == res.status_code
        self.agents_categories = res.json()['data']

    def get_agent_configuration(self, publicId, configType='openvpn'):
        res = requests.get(
            self.discovered['AgentConfiguration'].replace('{publicId}', publicId).replace('{configType}',
                                                                                          configType) + '?fields=*',
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            })

        assert 200 == res.status_code
        self.agents_configuration = res.json()['data']

    def get_agent_configuration_router_add_subnet_list(self, agentId):
        res = requests.get(
            self.discovered['AgentConfigurationRouterAddSubnetList'].replace('{agentId}', agentId) + '?fields=*',
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            })

        assert 200 == res.status_code
        return res.json()['data']

    def get_agent_configuration_router_port_fwd_list(self, agentId):
        res = requests.get(
            self.discovered['AgentConfigurationRouterPortFwdList'].replace('{agentId}', agentId) + '?fields=*',
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            })

        assert 200 == res.status_code
        return res.json()['data']

    def get_agent_device_list(self, agentId):
        res = requests.get(
            self.discovered['AgentDeviceList'].replace('{agentId}', agentId) + '?fields=*',
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            })

        assert 200 == res.status_code
        return res.json()['data']

    def get_agent_vpn_usage(self, agentId):
        res = requests.get(
            self.discovered['AgentVpnUsage'].replace('{agentId}', agentId),
            headers={
                'IXapi-Version': self.api_version,
                'IXapi-Application': self.api_application,
                'Authorization': self.token_authorization,
                'IXapi-Company': self.companies[0]['publicId']
            })
        assert 200 == res.status_code
        return res.json()['data']

    def network_mask_to_bits(self, network_mask):
        return sum(bin(int(x)).count('1') for x in network_mask.split('.'))


if __name__ == '__main__':
    with open('config.json') as config_file:
        conf = json.load(config_file)

    i = Ixon(conf['api_application'])
    i.generate_token(conf['email'], conf['password'])
    i.discovery()

    i.get_companies()
    i.get_agents()

    for x in i.agents:
        aux = i.get_agent_ips(x['publicId'])
        if aux is not None:
            print('%s [%s]: %s' % (x['name'], x['publicId'], aux))
