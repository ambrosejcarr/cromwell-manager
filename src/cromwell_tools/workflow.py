from google.cloud import storage
from .task import Task
from .cromwell import Cromwell


# todo implement timing
class Workflow:
    """Object to define an instance of a workflow run on Cromwell."""

    def __init__(self, run_id, cromwell_server, storage_client=None):
        """Defines a Cromwell-runnable WDL workflow.

        :param str run_id: hash code for this workflow
        :param Cromwell cromwell_server: an authenticated cromwell server object
        """
        self._cromwell_server = cromwell_server
        self.run_id = run_id
        self._storage_client = storage_client

        # filled by querying server
        self._tasks = {}

    @classmethod
    def from_submission(cls, wdl, inputs_json, options_json, cromwell_server, *args, **kwargs):
        """Submit a new workflow, returning a Workflow object.

        :param str wdl: wdl that defines this workflow
        :param str inputs_json: inputs to this wdl
        :param Cromwell cromwell_server: an authenticated cromwell server

        :param str options_json: options file for the workflow
        :param bool wait: if True, wait until workflow recognizes as submitted (default: True)
        :param int timeout: maximum time to wait
        :param int delay: time between status queries
        :param bool verbose: if True, print request results
        :param args: additional positional args to pass to requests.post
        :param kwargs: additional keyword args to pass to request.post

        :return dict: Cromwell submission result
        """
        files = cls._create_submission_json(wdl, inputs_json, options_json)
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
    def _create_submission_json(wdl, inputs_json, options_json):
        return {
            'wdlSource': open(wdl, 'rb'),
            'workflowInputs': open(inputs_json, 'rb'),
            'workflowOptions': open(options_json, 'rb')
        }

    @property
    def status(self):
        """Status of workflow."""
        return self.cromwell_server.status(self.run_id).json()

    @property
    def metadata(self):
        """Workflow metadata."""
        return self.cromwell_server.metadata(self.run_id).json()

    def abort(self, *args, **kwargs):
        """Abort this workflow.

        :param args: arguments to pass to cromwell.abort
        :param kwargs: keyword arguments to pass to cromwell.abort

        :return request.Response: abort response
        """
        return self.cromwell_server.abort_workflow(self.run_id, *args, **kwargs).json()

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
        self.cromwell_server.timing(self.run_id)

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
        self.cromwell_server.wait_for_status(complete_status, self.run_id, *args, **kwargs)

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
