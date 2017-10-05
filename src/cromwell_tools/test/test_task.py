import unittest
import cromwell_tools as cwt
import os
import json

# todo can scrape this from your cromwell config file, if that's in a standard place
ip_address = 'http://localhost:6361'
module_dir, module_name = os.path.split(__file__)

# grab a successful run id
successful_run_id = '884d3bb0-20b6-455e-8ecb-106e7d287e81'
successful_large_workflow_id = 'ed9485fb-3c9a-4b6a-8ac4-56dd4cd4cd41'


class TestTask(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # initialize a testing workflow
        cls.wdl = module_dir + '/data/testing.wdl'
        cls.inputs = module_dir + '/data/testing_example_inputs.json'
        cls.options = module_dir + '/data/options.json'
        cls.metadata = module_dir + '/data/example_metadata.json'
        cls.cromwell = cwt.Cromwell(ip_address)

    def test_workflow_monitoring_on_completed_run(self, verbose=True):
        workflow = cwt.Workflow.(successful_run_id, self.cromwell)
        workflow.save_resource_utilization('test_utilization.txt', verbose=verbose)

    def test_workflow_monitoring_on_completed_large_run(self, verbose=True):
        workflow = cwt.Workflow.from_submission(self.wdl, self.inputs, self.options, self.cromwell)
        workflow.run_id = successful_large_workflow_id
        workflow.save_resource_utilization('test_utilization.txt', verbose=verbose)


if __name__ == "__main__":
    unittest.main()
