import unittest
import cromwell_tools as cwt

# todo can scrape this from your cromwell config file, if that's in a standard place
ip_address = 'http://localhost:6361'


class TestCromwell(unittest.TestCase):

    def test_server_running(self):
        # make sure the server is running
        server = cwt.Cromwell(ip_address)
        self.assertTrue(server.server_is_running(server.ip_address))
