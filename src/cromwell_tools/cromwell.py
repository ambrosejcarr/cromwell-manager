import re
import requests
import json
from collections.abc import Iterable
from time import sleep


class Cromwell:
    def __init__(self, ip_address, username=None, password=None, version='v1'):
        self._ip_address = ip_address
        self.username = username
        self.password = password
        self.version = version
        self.url_prefix = '{ip}/api/workflows/{version}'.format(
            ip=self.ip_address, version=self.version)

    @property
    def ip_address(self):
        return self._ip_address

    @ip_address.setter
    def ip_address(self, value):
        if not re.match('https?://', value):
            raise ValueError('ip address must be an http or https address.')
        if not self.server_is_running(value):
            raise ValueError('ip address does not match a running cromwell server')
        self._ip_address = value.rstrip('/')  # trailing slash is not accepted by cromwell

    @staticmethod
    def print_request(request_type, request_string, response):
        try:
            formatted_response = json.dumps(response.json(), indent=2)
            print('{request_type} Request: {request_string}\nResponse: {response}\n'
                  'Response Content:\n{response_content}'.format(
                    request_type=request_type, request_string=request_string,
                    response=response.status_code, response_content=formatted_response))
        except json.decoder.JSONDecodeError:  # no content obtained
            print('{request_type} Request: {request_string}\nResponse: {response}\n'
                  .format(request_type=request_type, request_string=request_string,
                          response=response.status_code))

    @staticmethod
    def raise_failure(response, message):
        raise RuntimeError(
            '{message}\n'
            'Response Code: {code}\n'
            'Reason: {reason}\n'.format(
                message=message,
                code=response.status_code,
                reason=response.reason
            )
        )

    def wait_for_status(self, status, run_id, verbose=False, timeout=15, delay=3):
        """

        :param iterable | str status:
        :param run_id:
        :param verbose:
        :param timeout:
        :param delay:
        :return:
        """
        if isinstance(status, str):
            status = {status}
        elif isinstance(status, Iterable):
            status = set(status)
        else:
            raise TypeError('status must be a str or iterable of strings')
        if verbose:
            print('Waiting for workflow to achieve {status} status ...'
                  .format(status=status))

        # todo check timeout + delay re valid combination
        response = None
        if timeout:
            for _ in range(0, timeout, delay):
                response = self.status(run_id)
                if response.json()['status'] in status:
                    return response
                sleep(delay)
        else:
            while True:
                response = self.status(run_id)
                if response.json()['status'] in status:
                    return response
                sleep(delay)

        # workflow didn't start
        message = ('Workflow took more than {n!s} seconds to achieve {status}'
                   ''.format(n=timeout, status=status))
        self.raise_failure(response, message)

    # todo think about making these decorators so that verbose doesn't need passing in.
    def post(self, url, verbose=False, *args, **kwargs):
        response = requests.post(url, *args, **kwargs)
        if verbose:
            self.print_request('POST', url, response)
        return response

    def get(self, url, verbose=False, *args, **kwargs):
        response = requests.get(url, *args, **kwargs)
        if verbose:
            self.print_request('GET', url, response)
        return response

    def server_is_running(self, verbose=False):
        return True if self.get(self.ip_address, verbose=verbose).status_code == 200 else False

    def get_completed_workflows(self):
        raise NotImplementedError

    def abort_workflow(self, workflow_id, verbose=False):
        url = self.url_prefix + '/{id}/abort'.format(id=workflow_id)
        return self.post(url, verbose=verbose)

    def submit(self, files, wait=True, verbose=False, timeout=15, delay=3):
        submit_response = self.post(self.url_prefix, verbose=verbose, files=files)
        if submit_response.status_code > 201:
            self.raise_failure(submit_response, 'Workflow failed to start!')
        if wait:
            self.wait_for_status(
                status={'Running', 'Submitted', 'Succeeded'},  # todo change to "wait until not status"
                run_id=submit_response.json()['id'],
                timeout=timeout, delay=delay, verbose=verbose)
        return submit_response

    def batch(self):
        raise NotImplementedError

    def outputs(self, workflow_id, verbose=False):
        url = self.url_prefix + '/{id}/outputs'.format(id=workflow_id)
        return self.get(url, verbose=verbose)

    def query(self):
        raise NotImplementedError

    def status(self, workflow_id, verbose=False):
        url = self.url_prefix + '/{id}/status'.format(id=workflow_id)
        return self.get(url, verbose=verbose)

    def logs(self, workflow_id, verbose=False):
        url = self.url_prefix + '/{id}/logs'.format(id=workflow_id)
        return self.get(url, verbose=verbose)

    def metadata(self, workflow_id, verbose=False):
        url = self.url_prefix + '/{id}/metadata'.format(id=workflow_id)
        return self.get(url, verbose=verbose)

    def backends(self, verbose=False):
        return self.get(self.url_prefix + 'backends', verbose=verbose)
