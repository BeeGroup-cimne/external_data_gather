import base64
import json

import requests


# https://developer.ixon.cloud/docs/how-to-use-the-apiv2
class Ixon:
    def __init__(self, application_id, token='', public_id=''):
        self.api_version = '2'
        self.api_url = 'https://portal.ixon.cloud/api'
        self.application_id = application_id
        self.token = token
        self.publicId = public_id
        self.companies = []
        self.agents = []

    def generate_token(self, email, password, otp=''):
        encoded_str = base64.b64encode(bytes(f"{email}:{otp}:{password}", 'utf-8')).decode('utf-8')

        headers = {"Api-Version": self.api_version,
                   "Api-Application": self.application_id,
                   "Content-Type": "application/json",
                   "Authorization": f"Basic {encoded_str}"
                   }

        res = requests.post(url=self.api_url + '/access-tokens', headers=headers,
                            data=json.dumps({"expiresIn": 5184000}), timeout=(5, 20))  # expiresIn (seconds) max: 60d

        assert 201 == res.status_code

        res = res.json()
        self.token = res['data']['secretId']
        self.publicId = res['data']['publicId']

    def get_companies(self):
        headers = {
            "Content-Type": "application/json",
            "Api-Version": self.api_version,
            "Api-Application": self.application_id,
            "Authorization": f"Bearer {self.token}"
        }

        res = requests.get(url=self.api_url + f'/companies?fields=publicId,name', headers=headers)

        assert 200 == res.status_code

        res = res.json()
        self.companies = res['data']

    def get_agents(self, limit=1000):
        headers = {
            "Content-Type": "application/json",
            "Api-Version": self.api_version,
            "Api-Application": self.application_id,
            "Authorization": f"Bearer {self.token}",
            "Api-Company": self.companies[0]['publicId']
        }

        moreAfter = True

        while moreAfter:
            if moreAfter is True:
                res = requests.get(url=self.api_url + f'/agents?page-size={limit}', headers=headers)
            else:
                res = requests.get(url=self.api_url + f'/agents?page-size={limit}&page-after={moreAfter}',
                                   headers=headers)

            res = res.json()
            moreAfter = res['moreAfter']
            self.agents.extend(res['data'])

    def get_network_config(self, agent):
        headers = {
            "Api-Version": '2',
            "Api-Application": self.application_id,
            'Authorization': f"Bearer {self.token}",
            "Api-Company": self.companies[0]['publicId']
        }

        res = requests.get(
            url=f"https://portal.ixon.cloud/api/agents/{agent}?fields=activeVpnSession.vpnAddress,config.routerLan.*,devices.*,devices.dataProtocol.*,deviceId,description",
            headers=headers,
            timeout=20)
        assert 200 == res.status_code
        return res.json()


if __name__ == '__main__':
    i = Ixon(application_id='<application_id>')
    i.generate_token('<user>', '<password>')
    i.get_companies()
    i.get_agents()

    for j in i.agents[:]:
        res = i.get_network_config(j['publicId'])

        if 'data' in res:
            data = res['data']
            if 'deviceId' in data:
                if data['activeVpnSession'] and data['config'] and data['config']['routerLan']:
                    values = {'ip_vpn': data['activeVpnSession']['vpnAddress'],
                              'network': data['config']['routerLan']['network'],
                              'deviceId': data['deviceId'],
                              'network_mask': data['config']['routerLan']['netMask']}
                    print(values)
