import unittest
import cromwell_tools
import os

module_dir, module_name = os.path.split(__file__)


class TestWorkflow(unittest.TestCase):

    def test_workflow(self):
        wdl = module_dir + '/data/testing.wdl'
        inputs = module_dir + '/data/testing_example_inputs.json'
        options = module_dir + '/data/options.json'

        workflow = cromwell_tools.Workflow(wdl, inputs, options)


