import json
import tempfile
from google.cloud import storage
from .task import Task
from .cromwell import Cromwell
from .io_util import GSObject, HTTPObject


# todo add typechecking
class Workflow:
    """Object to define an instance of a workflow run on Cromwell."""

    def __init__(self, workflow_id, cromwell_server, storage_client=None):
        """Defines a Cromwell-runnable WDL workflow.

        :param str workflow_id: hash code for this workflow
        :param Cromwell cromwell_server: an authenticated cromwell server object
        """
        self.id = workflow_id
        self._cromwell_server = cromwell_server
        self._storage_client = storage_client

        # filled by querying server
        self._tasks = {}

    @classmethod
    def from_submission(
            cls, wdl, inputs_json, cromwell_server, storage_client, options_json=None,
            workflow_dependencies=None, custom_labels=None, *args, **kwargs):
        """Submit a new workflow, returning a Workflow object.


        :param str wdl: wdl that defines this workflow
        :param str inputs_json: inputs to this wdl
        :param Cromwell cromwell_server: an authenticated cromwell server
        :param storage.Client storage_client: authenticated google storage client

        :param str workflow_dependencies:
        :param dict custom_labels:
        :param str options_json: options file for the workflow

        :param bool wait: if True, wait until workflow recognizes as submitted (default: True)
        :param int timeout: maximum time to wait
        :param int delay: time between status queries
        :param bool verbose: if True, print request results
        :param args: additional positional args to pass to requests.post
        :param kwargs: additional keyword args to pass to request.post

        :return dict: Cromwell submission result
        """
        files = cls._create_submission_json(
            wdl=wdl, inputs_json=inputs_json, options_json=options_json,
            workflow_dependencies=workflow_dependencies, custom_labels=custom_labels,
            gs_client=storage_client)

        response = cromwell_server.submit(files=files, *args, **kwargs)
        workflow = cls(run_id=response.json()['id'], cromwell_server=cromwell_server)
        return workflow

    @property
    def storage_client(self):
        """Authenticated google storage client."""
        if self._storage_client is None:
            self._storage_client = storage.Client()
        return self._storage_client

    @storage_client.setter
    def storage_client(self, value):
        if not isinstance(value, storage.Client):
            raise TypeError('storage_client must be a google.cloud.storage.Client object, not %s'
                            % type(value))
        self._storage_client = value

    @property
    def cromwell_server(self):
        """Authenticated, currently-running Cromwell server."""
        return self._cromwell_server

    @cromwell_server.setter
    def cromwell_server(self, server):
        if not isinstance(server, Cromwell):
            raise TypeError('server must be a Cromwell Server instance.')
        if not server.server_is_running():
            raise RuntimeError('server is not running')

    @staticmethod
    def _create_submission_json(wdl, inputs_json, gs_client, options_json=None,
                                workflow_dependencies=None, custom_labels=None):
        """Create a submission json for the submit POST request.

        :param str wdl:
        :param str inputs_json:
        :param storage.Client gs_client:

        :param str options_json:
        :param str workflow_dependencies:
        :param dict custom_labels:
        :return dict: json dictionary containing inputs: open filehandles
        """
        submission_json = {}

        for name, param in (('wdl', wdl), ('inputs_json', inputs_json)):
            if param is None:
                raise ValueError('parameter %s is required.' % name)

        check_parameters = {
            'wdlSource': wdl,
            'workflowInputs': inputs_json,
            'workflowOptions': options_json,
            'workflowDependencies': workflow_dependencies,
            'customLabels': custom_labels
        }

        for key, param in check_parameters.items():
            if param is not None:
                if param.startswith('gs://'):
                    submission_json[key] = GSObject(param, gs_client).download_to_bytes_readable()
                elif any(param.startswith(prefix) for prefix in ('https://', 'http://')):
                    submission_json[key] = HTTPObject(param).download_to_bytes_readable()
                elif isinstance(param, dict):
                    fileobj = tempfile.TemporaryFile(mode='w+b')
                    fileobj.write(json.dumps(param).encode())
                    fileobj.seek(0)
                    submission_json[key] = fileobj
                else:
                    submission_json[key] = open(param, 'rb')  # todo filecheck this (?)

    @property
    def status(self):
        """Status of workflow."""
        return self.cromwell_server.status(self.id).json()

    @property
    def metadata(self):
        """Workflow metadata."""
        return self.cromwell_server.metadata(self.id).json()

    @property
    def root(self):
        """root directory for workflow outputs"""
        return self.metadata['workflowRoot']

    @property
    def outputs(self):
        """workflow outputs"""
        return self.cromwell_server.outputs(self.id).json()

    @property
    def inputs(self):
        """workflow inputs"""
        return self.metadata['inputs']

    @property
    def logs(self):
        """workflow logs"""
        return self.cromwell_server.logs(self.id).json()

    def abort(self, *args, **kwargs):
        """Abort this workflow.

        :param args: arguments to pass to cromwell.abort
        :param kwargs: keyword arguments to pass to cromwell.abort

        :return request.Response: abort response
        """
        return self.cromwell_server.abort_workflow(self.id, *args, **kwargs).json()

    def tasks(self, retrieve=True):
        """Get the workflow task summaries.

        :param bool retrieve: if True, get the current status from Cromwell, otherwise retrieve
          stored status (default True)

        :return dict: Cromwell metadata for workflow
        """
        if retrieve:
            self._tasks = {
                name: Task(call_data, self.storage_client) for name, call_data in
                self.metadata['calls'].items()}
        return self._tasks

    def timing(self):
        """Open timing for this task in browser window."""
        self.cromwell_server.timing(self.id)

    def wait_until_complete(self, *args, **kwargs):
        """Wait until the workflow completes running.

        Optional Arguments:
        :param str run_id: identifier hash code for a workflow
        :param bool verbose: if True, print the requests made
        :param int timeout: maximum time to wait
        :param int delay: time between status queries

        :return requests.Response: status response from Cromwell
        """
        complete_status = ['Aborted', 'Failed', 'Succeeded']
        self.cromwell_server.wait_for_status(complete_status, self.id, *args, **kwargs)

    def save_resource_utilization(self, filename, retrieve=True):
        """Save resource utilizations for each task to file.

        :param str filename: filename to save resource utilization

        :param bool retrieve: if True, get the current metadata from Cromwell, otherwise retrieve
          stored metadata (default True)
        :param str run_id: identifier hash code for a workflow
        :param bool verbose: if True, print the requests made
        :param int timeout: maximum time to wait
        :param int delay: time between status queries

        :return requests.Response: status response from Cromwell
        """
        with open(filename, 'w') as f:
            for task in self.tasks(retrieve=retrieve).values():
                f.write(str(task.resource_utilization))

    # todo debug this; would be nice to get a list of currently-running tasks
    # def running_tasks(self):
    #     if self.status['status'] != 'Running':
    #         print('Workflow is not running')
    #         return
    #     else:
    #         running = []
    #         for name, call in self.metadata['calls'].items():
    #             for shard in call:
    #                 if shard['executionStatus'] == 'Running':
    #                     running.append(name)
    #         return self.metadata['calls'][-1]['name']
