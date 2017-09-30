import unittest
import cromwell_tools as cwt
import subprocess
import os

# todo can scrape this from your cromwell config file, if that's in a standard place
ip_address = 'http://localhost:6361'
module_dir, module_name = os.path.split(__file__)

# grab a successful run id
successful_run_id = '884d3bb0-20b6-455e-8ecb-106e7d287e81'
successful_large_workflow_id = 'ed9485fb-3c9a-4b6a-8ac4-56dd4cd4cd41'


class TestWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # initialize a testing workflow
        cls.wdl = module_dir + '/data/testing.wdl'
        cls.inputs = module_dir + '/data/testing_example_inputs.json'
        cls.options = module_dir + '/data/options.json'
        cls.server = cwt.Cromwell(ip_address)

    def test_status(self, verbose=True):
        # test that old workflow obtains succeeded state
        workflow = cwt.Workflow(self.wdl, self.inputs, self.options, self.server)
        workflow.run_id = successful_run_id
        status = workflow.status(verbose=verbose)
        self.assertEqual(status, 'Succeeded')

    def test_submit(self):
        # start a new run, verify it is submitted successfully
        workflow = cwt.Workflow(self.wdl, self.inputs, self.options, self.server)
        response = workflow.submit(wait=True, verbose=True)
        workflow.wait_until_complete(verbose=True)
        self.assertLessEqual(response.status_code, 201)
        self.assertIn(response.json()['status'], {'Submitted', 'Running'})

    def test_unrun_workflow_raises_exception(self, verbose=True):
        workflow = cwt.Workflow(self.wdl, self.inputs, self.options, self.server)
        self.assertRaises(RuntimeError, workflow.status, verbose)

    def test_metadata(self, verbose=True):
        # test that old workflow obtains succeeded state
        workflow = cwt.Workflow(self.wdl, self.inputs, self.options, self.server)
        workflow.run_id = successful_run_id
        metadata = workflow.metadata(verbose=verbose)
        self.assertEqual(metadata['status'], 'Succeeded')
        self.assertEqual(metadata['workflowName'], 'Sleep')

    @unittest.skip('long-running test (~1m)')
    def test_small_workflow_monitoring(self):
        workflow = cwt.Workflow(self.wdl, self.inputs, self.options, self.server)
        workflow.submit(wait=True, verbose=True)
        workflow.wait_until_complete(verbose=True)
        workflow.save_resource_utilization(retrieve=True, filename='test_utilization.txt')

    @unittest.skip('long-running test (~60m)')
    def test_large_workflow_monitoring(self):

        # download wdl
        count_wdl_name = module_dir + '/data/10x_count.wdl'
        if not os.path.isfile(count_wdl_name):
            dl_10x_count_args = [
                'curl', '-o', count_wdl_name,
                'https://raw.githubusercontent.com/HumanCellAtlas/skylab/master/10x/'
                'count/count.wdl']
            subprocess.check_output(dl_10x_count_args)

        # download inputs
        count_inputs_name = module_dir + '/data/10x_count_inputs.json'
        if not os.path.isfile(count_inputs_name):
            dl_10x_count_inputs_args = [
                'curl', '-o', count_inputs_name,
                'https://raw.githubusercontent.com/HumanCellAtlas/skylab/master/10x/'
                'count/example_count_input.json']
            subprocess.check_output(dl_10x_count_inputs_args)

        # run the workflow, monitor.
        workflow = cwt.Workflow(count_wdl_name, count_inputs_name, self.options,
                                self.server)
        workflow.submit(wait=True, verbose=True)
        workflow.wait_until_complete(verbose=True, delay=60)
        workflow.save_resource_utilization(retrieve=True, filename='test_utilization.txt')

if __name__ == "__main__":
    unittest.main()
