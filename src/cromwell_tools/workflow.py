from google.cloud import storage
from .task import Task
from .cromwell import Cromwell


class Workflow:

    def __init__(self, wdl, inputs_json, options_json, cromwell_server):
        self.wdl = wdl
        self.inputs_json = inputs_json
        self.options_json = options_json
        self._cromwell_server = cromwell_server

        # filled on-demand
        self._storage_client = None

        # filled by querying server
        self.run_id = None
        self._metadata = None
        self._tasks = {}
        self._status = None

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = storage.Client()
        return self._storage_client

    @property
    def cromwell_server(self):
        return self._cromwell_server

    @cromwell_server.setter
    def cromwell_server(self, server):
        if not isinstance(server, Cromwell):
            raise TypeError('server must be a Cromwell Server instance.')
        if not server.server_is_running():
            raise RuntimeError('server is not running')

    @property
    def _submission_json(self):
        return {
            'workflowSource': open(self.wdl, 'rb'),
            'workflowInputs': open(self.inputs_json, 'rb'),
            'workflowOptions': open(self.options_json, 'rb')
        }

    def submit(self, verbose=False, wait=True, timeout=15, delay=3):
        response = self.cromwell_server.submit(
            files=self._submission_json, verbose=verbose,
            timeout=timeout, delay=delay, wait=wait)
        self.run_id = response.json()['id']
        return response

    def abort(self, verbose=False):
        raise NotImplementedError

    # todo should be able to refactor using some kind of factory function or decorator
    def status(self, retrieve=True, verbose=False):
        if self.run_id is None:
            raise RuntimeError(
                'Cannot check status on workflow that has not been run. '
                'Use `.submit()` to run this workflow.')
        if self._status is None or retrieve:
            response = self.cromwell_server.status(self.run_id, verbose=verbose)
            self._status = response.json()['status']
        return self._status

    def metadata(self, retrieve=True, verbose=False):
        if self._metadata is None or retrieve:
            response = self.cromwell_server.metadata(self.run_id, verbose=verbose)
            self._metadata = response.json()
        return self._metadata

    def tasks(self, retrieve=True, verbose=False):
        if self._tasks is None or retrieve:
            metadata = self.metadata(retrieve=retrieve, verbose=verbose)
            self._tasks = {
                name: Task(call_data, self.storage_client) for name, call_data in
                metadata['calls'].items()}
        return self._tasks

    def wait_until_complete(self, verbose=False, delay=10, timeout=None):
        completion_status = {'Aborted', 'Failed', 'Succeeded'}
        self.cromwell_server.wait_for_status(
            completion_status, self.run_id, verbose=verbose, timeout=timeout, delay=delay)

    def save_resource_utilization(self, filename, retrieve=True, verbose=False):
        with open(filename, 'w') as f:
            for task in self.tasks(retrieve=retrieve, verbose=verbose).values():
                f.write(str(task.resource_utilization))
