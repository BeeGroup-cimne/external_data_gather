import base64
import json

import requests


class Ixon:
    def __init__(self, application_id):
        self.api_version = '2'
        self.api_url = 'https://portal.ixon.cloud/api'
        self.application_id = application_id
        self.token = None
        self.publicId = None
        self.companies = None
        self.agents = None

    def generate_token(self, email, password, otp=''):
        encoded_str = base64.b64encode(bytes(f"{email}:{otp}:{password}", 'utf-8')).decode('utf-8')

        headers = {"Api-Version": self.api_version,
                   "Api-Application": self.application_id,
                   "Content-Type": "application/json",
                   "Authorization": f"Basic {encoded_str}"
                   }

        res = requests.post(url=self.api_url + '/access-tokens', headers=headers,
                            data=json.dumps({"expiresIn": 60 * 2}), timeout=(5, 20))

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

    def get_agents(self):
        headers = {
            "Content-Type": "application/json",
            "Api-Version": self.api_version,
            "Api-Application": self.application_id,
            "Authorization": f"Bearer {self.token}",
            "Api-Company": self.companies[0]['publicId']
        }

        res = requests.get(url=self.api_url + f'/agents', headers=headers)

        assert 200 == res.status_code

        res = res.json()

        self.agents = res['data']


if __name__ == '__main__':
    with open('config.json') as config_file:
        conf = json.load(config_file)

    i = Ixon(application_id=conf['api_application'])
    i.generate_token(conf['email'], conf['password'])
    i.get_companies()
    i.get_agents()
