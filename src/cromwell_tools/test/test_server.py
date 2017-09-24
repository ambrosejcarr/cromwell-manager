import unittest
import cromwell_tools

ip_address = 'http://localhost:6361'


class TestCromwell(unittest.TestCase):

    def test_server_running(self):
        cw = cromwell_tools.Server(ip_address)
        self.assertTrue(cw.server_is_running(cw.ip_address))

    def test_cromwell_test_run(self):
        pass
