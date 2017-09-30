import pyparsing as pp
from itertools import zip_longest
from collections import namedtuple

def log_parser(log):
    parser = pp.OneOrMore(pp.Group(
        pp.Word(pp.alphanums + '_').resultsName('task_name') +
        'Monitoring Summary:' +
        'Max Memory Usage (MB):' + pp.Word(pp.nums).setResultsName('max_memory') +
        'Available Memory (MB):' + pp.Word(pp.nums).setResultsName('available_memory') +
        'Max disk usage   (KB):' + pp.Word(pp.nums).setResultsName('max_disk') +
        'Available disk   (KB):' + pp.Word(pp.nums).setResultsName('available_disk') +
        'Memory Utilized   (%):' + pp.Word(pp.nums + '.').setResultsName('utilized_memory') +
        'Disk Utilized     (%):' + pp.Word(pp.nums + '.').setResultsName('utilized_disk') +
        pp.Literal('Robust Estimate?     :') + pp.Word(pp.alphanums).setResultsName('robust_estimate')
    ))
    return parser

UtilizationResult = namedtuple(
    'UtilizationResult',
    ['name', 'max_mem', 'available_mem', 'max_disk', 'available_disk', 'disk_utilized',
     'mem_utilized', 'robust'])

def log_parser(log):

    def grouper(iterable, fillvalue=None):
        args = [iter(iterable)] * 8
        return zip_longest(*args, fillvalue=fillvalue)

    for result in grouper(log):
        task_name = result[0].split()[0]
        max_mem = int(result[1].split()[-1])
        available_mem = int(result[2].split()[-1])
        max_disk = int(result[3].split()[-1])
        available_disk = int(result[4].split()[-1])
        disk_utilized = float(result[5].split()[-1])
        mem_utilized = float(result[6].split()[-1])
        robust = True if result[7].split()[-1] == 'True' else False
        utilization_result = UtilizationResult(
            task_name, max_mem, available_mem, max_disk, available_disk, disk_utilized,
            mem_utilized, robust)

        # todo group by task


class ResourceScaling:

    def __init__(self, logs):
        """

        :param dict logs: {input_size: log}
        """
        self.logs = logs

    def aggregate(self):
        """aggregate resource utilization by task"""

        for log in self.logs:
            with open(log, 'r') as f:
                pass

