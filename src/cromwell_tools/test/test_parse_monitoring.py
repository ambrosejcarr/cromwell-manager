import unittest
import os
from ..parse_monitoring import parse_task_log


class TestParsing(unittest.TestCase):

    _directory, _ = os.path.split(__file__)
    data_file = _directory + '/data/example_monitoring.log'

    def test_parsing_generates_correct_numbers(self):
        with open(self.data_file, 'r') as f:
            log = f.readlines()
        used_memory, total_memory, robust_memory, used_disk, total_disk, robust_disk = (
            parse_task_log(log))
        self.assertEqual(used_memory, 126, 'max used memory should be 126')
        self.assertEqual(total_memory, 1700, 'total memory should be 1700')
        self.assertEqual(
            robust_memory, False, 'this should be False, there were only four estimates')
        self.assertEqual(used_disk, 23048, 'used disk should be 23048')
        self.assertEqual(total_disk, 10190136, 'total disk should be 10190136')
        self.assertEqual(robust_disk, False, 'this should be False, there were only four estimates')

if __name__ == "__main__":
    unittest.main()
