import unittest
import cromwell_tools
import os
from copy import deepcopy

# todo can scrape this from your cromwell config file, if that's in a standard place
ip_address = 'http://localhost:6361'
module_dir, module_name = os.path.split(__file__)


class TestCromwell(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # initialize a testing workflow
        wdl = module_dir + '/data/testing.wdl'
        inputs = module_dir + '/data/testing_example_inputs.json'
        options = module_dir + '/data/options.json'
        cls.workflow = cromwell_tools.Workflow(wdl, inputs, options)

    def test_server_running(self):
        cw = cromwell_tools.Server(ip_address)
        self.assertTrue(cw.server_is_running(cw.ip_address))

    def test_cromwell_test_run(self):
        cw = cromwell_tools.Server(ip_address)
        response = cw.start_workflow(self.workflow)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.content['status'], 'Submitted')

    def test_unrun_workflow_raises_exception(self):
        cw = cromwell_tools.Server(ip_address)
        workflow = deepcopy(self.workflow)
        workflow.run_id = '884d3bb0-20b6-455e-8ecb-106e7d287e81'
        _ = cw.check_status(workflow, verbose=True)
