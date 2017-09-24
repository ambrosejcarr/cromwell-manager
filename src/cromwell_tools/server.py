import requests
import re
# from requests.auth import HTTPBasicAuth


# todo factor in authentication somehow
class Server:
    def __init__(self, ip_address, username=None, password=None):
        self._ip_address = ip_address
        self.username = username
        self.password = password

    @property
    def ip_address(self):
        return self._ip_address

    @ip_address.setter
    def ip_address(self, value):
        if not re.match('https?://', value):
            raise ValueError('ip address must be an http or https address.')
        if not self.server_is_running(value):
            raise ValueError('ip address does not match a running cromwell server')
        self._ip_address = value

    @staticmethod
    def server_is_running(ip_address):
        response = requests.get(ip_address)
        return True if response.status_code == 200 else False

    def run_workflow(self, workflow):
        pass
