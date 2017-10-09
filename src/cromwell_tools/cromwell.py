import re
import json
import webbrowser
from time import sleep
from collections.abc import Iterable
from itertools import repeat
import requests
from requests.auth import HTTPBasicAuth


class Cromwell:
    """Wrapper for the Cromwell REST API"""

    def __init__(self, cromwell_url, username=None, password=None, api_version='v1'):
        """API wrapper for a running cromwell server

        :param str cromwell_url: url of a running cromwell instance
        :param str | None username: (optional) username for the cromwell instance
        :param str | None password: (optional) password for the cromwell instance
        :param str api_version: version of the cromwell API
        """

        if isinstance(cromwell_url, str):
            self._cromwell_url = cromwell_url
        else:
            raise TypeError('cromwell_url must be a str, not %s' % type(cromwell_url))

        if isinstance(username, str) or username is None:
            self.username = username
        else:
            raise TypeError('If provided, username must be a str, not %s' % type(username))

        if isinstance(password, str) or username is None:
            self.password = password
        else:
            raise TypeError('If provided, password must be a str, not %s' % type(password))

        if isinstance(api_version, str):
            self.api_version = api_version
        else:
            raise ValueError('version must be a str, not %s' % type(api_version))

        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.url_prefix = '{cromwell_url}/api/workflows/{version}'.format(
            cromwell_url=self.cromwell_url, version=self.api_version)

        # check that server is running
        if not self.server_is_running():
            raise RuntimeError('url, username, and password did not authenticate to a running '
                               'cromwell instance.')

    @property
    def cromwell_url(self):
        """URL for the cromwell REST endpoints."""
        return self._cromwell_url

    @cromwell_url.setter
    def cromwell_url(self, value):
        if not re.match('https?://', value):
            raise ValueError('cromwell_url must be an http or https address.')
        if not self.server_is_running(value):
            raise ValueError('cromwell_url does not match a running cromwell server')
        self._cromwell_url = value.rstrip('/')  # trailing slash is not accepted by cromwell

    @staticmethod
    def print_request(request_type, request_string, response):
        """Print a request to console.

        Triggered by the verbose=True flag on cromwell or workspace functions and properties.

        :param str request_type: {GET, POST} type of REST operation
        :param str request_string: full request url
        :param requests.Response response: response from request operation
        """
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
    def print_failure(response, message=''):
        """Print information on a failing request to console.

        :param requests.Response response: response from request operation
        :param str message: (optional) message to append to failure report
        """
        print(
            'Request: {url}\n'
            '{message}\n'
            'Response Code: {code}\n'
            'Reason: {reason}\n'.format(
                url=response.url, message=message, code=response.status_code, reason=response.reason
            ))

    def swagger(self):
        """Open the swagger page for this cromwell server."""
        webbrowser.open(self.cromwell_url)

    def wait_for_status(self, status, run_id, verbose=False, timeout=15, delay=3):
        """Wait until any status in a list of potentially many statuses is achieved for a workflow.

        :param Iterable status: Iterable of one or more statuses to wait for
        :param str run_id: identifier hash code for a workflow
        :param bool verbose: if True, print the requests made
        :param int timeout: maximum time to wait
        :param int delay: time between status queries
        :return requests.Response: response object generated when run_id achieves the first valid
          status
        """
        # raise error if a non-existent status is passed (infinite loop)
        status = set(status)
        if verbose:
            print('Waiting for workflow to achieve {status} status ...'
                  .format(status=status))

        response = None
        tries = range(0, timeout, delay) if timeout is not None else repeat(0, times=None)
        for _ in tries:
            response = self.status(run_id)
            if response.json()['status'] in status:
                return response
            sleep(delay)

        # workflow didn't start
        message = ('Workflow took more than {n!s} seconds to achieve {status}'
                   ''.format(n=timeout, status=status))
        self.print_failure(response, message)
        return response

    def post(self, url, verbose=False, *args, **kwargs):
        """Make a REST POST query to url.

        :param str url: POST query url

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param args: additional arguments to pass to requests.post
        :param kwargs: additional arguments to pass to requests.post
        :return requests.Response: requests response object
        """
        response = requests.post(url, auth=self.auth, *args, **kwargs)
        if verbose:
            self.print_request('POST', url, response)
        return response

    def get(self, url, verbose=False, open_browser=False, *args, **kwargs):
        """Make a REST GET query to url.

        :param str url: GET query url

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return requests.Response: requests response object
        """
        response = requests.get(url, auth=self.auth, *args, **kwargs)
        if verbose:
            self.print_request('GET', url, response)
        if open_browser:
            webbrowser.open(url)
        return response

    def server_is_running(self, *args, **kwargs):
        """Return True if the server is running, else False.

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        """
        return True if self.get(self.cromwell_url, *args, **kwargs).status_code == 200 else False

    def abort_workflow(self, workflow_id, *args, **kwargs):
        """Abort a workflow.

        :param str workflow_id: hash for workflow to abort

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param args: additional arguments to pass to requests.post
        :param kwargs: additional arguments to pass to requests.post
        :return response.Response: requests response object
        """
        url = self.url_prefix + '/{id}/abort'.format(id=workflow_id)
        return self.post(url, *args, **kwargs)

    def submit(self, files, wait=True, timeout=15, delay=3, verbose=False, *args, **kwargs):
        """Submit a new workflow.

        :param dict files: dictionary of files from workflow._submission_json

        :param bool wait: if True, wait until workflow recognizes as submitted
        :param int timeout: maximum time to wait
        :param int delay: time between status queries
        :param bool verbose: if True, print request results
        :param args: additional positional args to pass to requests.post
        :param kwargs: additional keyword args to pass to request.post
        :return response.Response: requests response object
        """
        submit_response = self.post(self.url_prefix, files=files, *args, **kwargs)
        if submit_response.status_code > 201:
            self.print_failure(submit_response, 'Workflow failed to start!')
            return submit_response
        if wait:
            self.wait_for_status(
                ['Running', 'Submitted', 'Succeeded'],
                run_id=submit_response.json()['id'],
                timeout=timeout, delay=delay, verbose=verbose)
        return submit_response

    def batch(self):
        raise NotImplementedError

    def outputs(self, workflow_id, *args, **kwargs):
        """Retrieve outputs for workflow_id.

        :param str workflow_id: hash for workflow to abort

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = self.url_prefix + '/{id}/outputs'.format(id=workflow_id)
        return self.get(url, *args, **kwargs)

    # todo add formatting to correct datetime string
    def query(self, start=None, end=None, names=None, ids=None, status=None, labels=None, *args,
              **kwargs):
        """Query cromwell for workflows matching specified metadata information.

        :param str start: datetime string in format #todo
        :param str end: datetime string in format #todo
        :param list names: list of one or more workflow name(s)
        :param list ids: list of one or more workflow id(s)
        :param list status: list of one or more workflow status(es). Must be a valid status:
          {Submitted, Running, Aborting, Failed, Succeeded, Aborted}
        :param dict labels: dictionary of custom label:value pairs

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :return requests.Response:
        """
        tags = []
        if start and isinstance(start, str):
            tags.append('start={}'.format(start))
        if end and isinstance(end, str):
            tags.append('end={}'.format(end))
        if names and isinstance(names, Iterable):
            tags.extend(('name={}'.format(n) for n in names))
        if ids and isinstance(ids, Iterable):
            tags.extend(('id={}'.format(i) for i in ids))
        if status and isinstance(status, Iterable):
            tags.extend(('status={}'.format(s) for s in status))
        if labels and isinstance(labels, dict):
            tags.extend(('{k}={v}'.format(k=k, v=v) for k, v in labels.items()))
        url = self.url_prefix + '/query?' + '&'.join(tags)
        return self.get(url, *args, **kwargs)

    # todo implement a way to filter queries by metadata information
    def filter(self, **kwargs):
        raise NotImplementedError

    def status(self, workflow_id, *args, **kwargs):
        """Retrieve status for workflow_id.

        :param str workflow_id: hash for workflow to abort

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = self.url_prefix + '/{id}/status'.format(id=workflow_id)
        return self.get(url, *args, **kwargs)

    def logs(self, workflow_id, *args, **kwargs):
        """Retrieve logs for workflow_id.

        :param str workflow_id: hash for workflow to abort

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = self.url_prefix + '/{id}/logs'.format(id=workflow_id)
        return self.get(url, *args, **kwargs)

    def metadata(self, workflow_id, *args, **kwargs):
        """Retrieve metadata for workflow_id.

        :param str workflow_id: hash for workflow to abort

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = self.url_prefix + '/{id}/metadata'.format(id=workflow_id)
        return self.get(url, *args, **kwargs)

    def backends(self, *args, **kwargs):
        """Retrieve backends for this cromwell instance.

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        return self.get(self.url_prefix + '/backends', *args, **kwargs)

    def timing(self, run_id):
        """Open timing in browser window for run_id.

        :param str run_id: run id to open timing for
        """
        webbrowser.open('{prefix}/{id}/timing'.format(prefix=self.url_prefix, id=run_id))

    def version(self, *args, **kwargs):
        """Retrieve the cromwell version

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = '{cromwell_url}/engine/{version}/version'.format(
            cromwell_url=self.cromwell_url, version=self.api_version)
        return self.get(url, *args, **kwargs)

    def stats(self, *args, **kwargs):
        """Retrieve cromwell statistics on number of running jobs

        :param bool verbose: if True, print the query, response code, and content (default False)
        :param bool open_browser: if True, display the GET result in browser (default False)
        :param args: additional positional args to pass to requests.get
        :param kwargs: additional keyword args to pass to request.get
        :return response.Response: requests response object
        """
        url = '{cromwell_url}/engine/{version}/stats'.format(
            cromwell_url=self.cromwell_url, version=self.api_version)
        return self.get(url, *args, **kwargs)
