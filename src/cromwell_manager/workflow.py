import sys
import os
import re
import json
import tempfile
from subprocess import Popen, PIPE, call
import datetime
import requests
from google.cloud import storage
from .calledtask import CalledTask
from .cromwell import Cromwell
from .io_util import (
    GSObject, HTTPObject, package_workflow_dependencies, check_exists, announce)


# todo generate links to google storage for inputs / outputs / files etc
class WorkflowBase:

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

    def __repr__(self):
        raise NotImplementedError

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

    # todo check that status can be called on a subworkflow
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

    def timing(self):
        """Open timing for this task in browser window."""
        self.cromwell_server.timing(self.id)

    def refresh_tasks(self):
        """update tasks in self.tasks"""
        for name, shard_list in self.metadata['calls'].items():
            if 'subWorkflowId' in shard_list[0]:  # is a list of subworkflows
                self._tasks[name] = [
                    SubWorkflow(m['subWorkflowId'], self.cromwell_server, self.storage_client) for m
                    in shard_list]
            else:
                self._tasks[name] = CalledTask(name, shard_list, self.storage_client)

    @property
    def tasks(self):
        """Get the workflow task summaries.

        :return dict: Cromwell metadata for workflow
        """
        if not self._tasks:
            self.refresh_tasks()
        return self._tasks

    def save_resource_utilization(self, filename, retrieve=True):
        """Save resource utilizations for each task to file.

        :param str | BufferedIOBase filename: filename or open file object in which to save
          resource utilization

        :param bool retrieve: if True, get the current metadata from Cromwell, otherwise retrieve
          stored metadata (default True)
        :param str workflow_id: identifier hash code for a workflow
        :param bool verbose: if True, print the requests made
        :param int timeout: maximum time to wait
        :param int delay: time between status queries

        :return requests.Response: status response from Cromwell
        """
        if isinstance(filename, str):
            filename = open(filename, 'w')
        for task in self.tasks.values():
            if isinstance(task, CalledTask):
                filename.write(str(task.resource_utilization))
            elif isinstance(task, SubWorkflow):
                task.save_resource_utilization(filename, retrieve=retrieve)  # recursion
            else:
                raise TypeError('tasks must be CalledTasks or Subworkflows, not %s'
                                % type(task))


# todo workflow fails to start, make the error messages clearer! right now you get a KeyError
# when Cromwell attempts to access Workflow ID (error should be thrown earlier)
# todo not sure storage client should be required
class Workflow(WorkflowBase):
    """Object to define an instance of a top-level workflow run on Cromwell."""

    def __repr__(self):
        return '<Workflow: %s>' % self.id

    @classmethod
    def from_submission(
            cls, wdl, inputs_json, cromwell_server, storage_client, options_json=None,
            workflow_dependencies=None, custom_labels=None, *args, **kwargs):
        """Submit a new workflow, returning a Workflow object.


        :param str wdl: wdl that defines this workflow
        :param str inputs_json: inputs to this wdl
        :param Cromwell cromwell_server: an authenticated cromwell server
        :param storage.Client storage_client: authenticated google storage client

        :param str | dict workflow_dependencies:
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
        workflow = cls(workflow_id=response.json()['id'], cromwell_server=cromwell_server,
                       storage_client=storage_client)
        return workflow

    @staticmethod
    def _create_submission_json(wdl, inputs_json, gs_client, options_json=None,
                                workflow_dependencies=None, custom_labels=None):
        """Create a submission json for the submit POST request.

        :param str wdl:
        :param str inputs_json:
        :param storage.Client gs_client:

        :param str options_json:
        :param str | dict workflow_dependencies:
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
            'customLabels': custom_labels
        }

        # workflow dependencies
        if workflow_dependencies is None:
            pass  # nothing to do
        elif isinstance(workflow_dependencies, str) and workflow_dependencies.endswith('.zip'):
            check_parameters['wdlDependencies'] = workflow_dependencies  # delay check to below
        elif isinstance(workflow_dependencies, dict):
            submission_json['wdlDependencies'] = package_workflow_dependencies(
                **workflow_dependencies)
        else:
            raise TypeError(
                'if provided, workflow_dependencies must be a dict containing '
                '(name, value) pairs, or a path to a pre-zipped dependency archive, '
                'not %s' % workflow_dependencies)

        for key, param in check_parameters.items():
            if param is not None:
                if isinstance(param, dict):  # custom labels or options json
                    fileobj = tempfile.TemporaryFile(mode='w+b')
                    fileobj.write(json.dumps(param).encode())
                    fileobj.seek(0)
                    submission_json[key] = fileobj
                elif param.startswith('gs://'):
                    submission_json[key] = GSObject(param, gs_client).download_to_bytes_readable()
                elif any(param.startswith(prefix) for prefix in ('https://', 'http://')):
                    submission_json[key] = HTTPObject(param).download_to_bytes_readable()
                else:
                    submission_json[key] = open(param, 'rb')

        return submission_json

    def abort(self, *args, **kwargs):
        """Abort this workflow.

        :param args: arguments to pass to cromwell.abort
        :param kwargs: keyword arguments to pass to cromwell.abort

        :return request.Response: abort response
        """
        return self.cromwell_server.abort_workflow(self.id, *args, **kwargs).json()

    def wait_until_complete(self, *args, **kwargs):
        """Wait until the workflow completes running.

        Optional Arguments:
        :param str workflow_id: identifier hash code for a workflow
        :param bool verbose: if True, print the requests made
        :param int timeout: maximum time to wait
        :param int delay: time between status queries

        :return requests.Response: status response from Cromwell
        """
        complete_status = ['Aborted', 'Failed', 'Succeeded']
        self.cromwell_server.wait_for_status(complete_status, self.id, *args, **kwargs)

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

    # todo untested; right now it's just validating every argument; needs to only do
    # files
    @classmethod
    def validate(
            cls, wdl, inputs_json,  storage_client, options_json=None,
            workflow_dependencies=None, custom_labels=None, *args, **kwargs):
        """Validate a workflow, catching errors before submission.

        if using positional arguments, the same argument set that is used for submission
        can be used to call `validate`.

        :param str wdl: wdl that defines this workflow
        :param str inputs_json: inputs to this wdl

        :param storage.Client storage_client: authenticated google storage client

        :param str | dict workflow_dependencies:
        :param dict custom_labels:
        :param str options_json: options file for the workflow

        :param args: argument sink for arguments to `from_submission` that are not used.
        :param kwargs: argument sink for arguments to `from_submission` that are not used.
        """

        file_dictionary = cls._create_submission_json(
            wdl=wdl, inputs_json=inputs_json, options_json=options_json,
            workflow_dependencies=workflow_dependencies, custom_labels=custom_labels,
            gs_client=storage_client)

        # create a temporary directory to organize the submission
        announce('creating temporary directory')
        with tempfile.TemporaryDirectory() as tmpdir:

            # change directory for validate
            old_wd = os.getcwd()
            os.chdir(tmpdir)

            if 'wdlDependencies' in file_dictionary:
                # dump the dependencies into the directory
                zipfile_data = file_dictionary['wdlDependencies'].read()
                announce('writing dependencies')
                with open(tmpdir + '/dependencies.zip', 'wb') as f:
                    f.write(zipfile_data)
                call(['unzip', '-o', '%s/dependencies.zip' % tmpdir, '-d', tmpdir])

            # write the wdl to the directory
            announce('writing wdl')
            wdl_data = file_dictionary['wdlSource'].read()
            with open(tmpdir + '/source.wdl', 'wb') as f:
                f.write(wdl_data)

            # run validate
            announce('running wdltool validate')
            try:
                cmd = 'java -jar {wdltool} validate {tmpdir}/source.wdl'.format(
                    tmpdir=tmpdir, wdltool=os.environ['wdltool']
                )
                p = Popen(cmd, stderr=PIPE, stdout=PIPE, shell=True)
                out, err = p.communicate()
                if err.strip():
                    print(err.decode())
                if out.strip():
                    print(out.decode())
                if not any((err.strip(), out.strip())):
                    announce('validation successful')

            except EnvironmentError:
                announce('wdltool.jar must be set as the environment var `wdltool` to '
                         'run validate')

            # # todo this doesn't work yet; need to select only File objects to check
            # # there is also complexity over Array[File] syntax; (how to check the json?)
            # print('CWM:{}:checking input file links'.format(datetime.datetime.now()))
            # input_data = json.load(file_dictionary['workflowInputs'])
            # for f in input_data:
            #     check_exists(f)

            # check that the docker images are available, else print a warning
            pattern = re.compile('runtime\s*?\{.*?docker:\s*?"(.*?)".*?\}', re.DOTALL)
            wdls = set(f for f in os.listdir(tmpdir) if f.endswith('.wdl'))
            dockers = []
            for wdl in wdls:
                with open('{}/{}'.format(tmpdir, wdl), 'r') as f:
                    data = f.read()
                    dockers.extend(re.findall(pattern, data))

            for image in set(dockers):
                if ':' in image:
                    name, tag = image.split(':')
                else:
                    name, tag = image, 'latest'

                # authenticate with docker and check if image exists
                auth_url = 'https://auth.docker.io/token'
                auth_service = 'registry.docker.io'
                reg_url = 'https://registry.hub.docker.com'

                tag_url = {
                    'reg_url': reg_url,
                    'repo': name,
                    'ref': tag,
                }
                querystring = {
                    'service': auth_service,
                    'scope': 'repository:%s:pull' % name,
                }

                auth_response = requests.request('GET', auth_url, params=querystring)
                auth_data = auth_response.json()

                reg_headers = {
                    'accept': "application/vnd.docker.distribution.manifest.v2+json",
                    'Authorization': 'Bearer %s' % auth_data['access_token']
                }
                get_manifests_v2 = "{reg_url}/v2/{repo}/manifests/{ref}".format(**tag_url)
                reg_response = requests.head(get_manifests_v2, headers=reg_headers)

                if reg_response.status_code == 200:
                    announce('checking docker image {}... OK.'.format(image))
                else:
                    announce('checking docker image {}... not found. Is image private?'
                             ''.format(image))

            # reset path
            os.chdir(old_wd)


class SubWorkflow(WorkflowBase):
    """A workflow without custom constructors"""

    def __repr__(self):
        return '<SubWorkflow: %s>' % self.id
