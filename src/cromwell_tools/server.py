import requests
import re
import json
# from requests.auth import HTTPBasicAuth


# todo factor in authentication somehow
class Server:
    def __init__(self, ip_address, username=None, password=None, version='v1'):
        self._ip_address = ip_address
        self.username = username
        self.password = password
        self.version = version

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
    def _print_request(request_response, failed=False):
        """print out status code and content of a request response.

        In case of failure, also print the reason
        # todo check if this can be automated (if requests interprets success/failure)

        :param request_response:
        :param bool failed: if the request failed, also print the reason
        :return:
        """
        # decode and recode with white space
        formatted_content = json.dumps(json.loads(request_response.content),
                                       indent=2, sort_keys=True)
        print('Status Code: {code}'.format(code=request_response.status_code))
        if failed:
            print('Reason: {reason}'.format(reason=request_response.reason))
        print('Content:\n{content}'.format(content=formatted_content))

    @classmethod
    def server_is_running(cls, ip_address, verbose=False):
        response = requests.get(ip_address)
        if verbose:
            cls._print_request(response)
        return True if response.status_code == 200 else False

    def start_workflow(self, workflow, verbose=False):
        response = requests.post(
            self.ip_address + '/api/workflows/{version}'.format(version=self.version),
            files=workflow.submission_json)
        if verbose:
            self._print_request(response)
        if response.status_code > 201:
            self._print_request(response, failed=True)
            raise ValueError('Workflow failed to start successfully')
        return response

    def check_status(self, workflow, verbose=False):
        response = requests.get(
            self.ip_address + '/api/workflows/{version}/{id}/status'.format(
                version=self.version, id=workflow.run_id)
        )
        if verbose:
            self._print_request(response)
        return response
