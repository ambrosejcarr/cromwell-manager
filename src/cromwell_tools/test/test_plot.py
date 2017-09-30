import unittest
import os
from cromwell_tools import plot

module_dir, module_name = os.path.split(__file__)
test_log = module_dir + '/data/utilizations/workflow4_utilization.txt'


class TestPlot(unittest.TestCase):

    def test_parser(self):
        raise NotImplementedError

if __name__ == "__main__":
    unittest.main()
