import unittest
import os
import json
import cromwell_tools as cwt

module_dir, module_name = os.path.split(__file__)
with open(module_dir + '/data/secrets.json', 'rb') as f:
    cromwell_config = json.load(f)


class TestCromwell(unittest.TestCase):

    def test_server_running(self):
        # make sure the server is running
        server = cwt.Cromwell(**cromwell_config)
        self.assertTrue(server.server_is_running(server.cromwell_url))
