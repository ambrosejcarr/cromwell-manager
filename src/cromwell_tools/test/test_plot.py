import unittest
import os
from cromwell_tools import plot

module_dir, module_name = os.path.split(__file__)

test_log = module_dir + '/../../../utilizations/workflow4_utilization.txt'


class TestPlot(unittest.TestCase):

    def test_parser(self):
        with open(test_log, 'r') as f:
            plot.log_parser(f)

if __name__ == "__main__":
    unittest.main()
