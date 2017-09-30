from itertools import zip_longest
from collections import namedtuple, defaultdict
from scsequtil.plot import grid
from operator import itemgetter


UtilizationResult = namedtuple(
    'UtilizationResult',
    ['name', 'max_mem', 'available_mem', 'max_disk', 'available_disk', 'disk_utilized',
     'mem_utilized', 'robust'])


class ResourceScaling:

    def __init__(self, logs):
        """

        :param dict logs: {input_size: log}
        """
        self._logs = logs
        self._utilization = None

    @property
    def logs(self):
        return self._logs

    @logs.setter
    def logs(self, value):
        if isinstance(value, dict):
            self._logs = value
        else:
            raise TypeError('Logs must be a dictionary of input_size: value')

    @property
    def utilization(self):
        return self._utilization

    @staticmethod
    def parse(log):

        def grouper(iterable, fillvalue=None):
            args = [iter(iterable)] * 8
            return zip_longest(*args, fillvalue=fillvalue)

        for result in grouper(log):
            # parse the fields in each section
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

            yield utilization_result

    def aggregate(self):
        """aggregate resource utilization by task"""

        self._utilization = defaultdict(list)

        for input_size, log in self.logs.items():
            with open(log, 'r') as f:
                for task in self.parse(f):
                    self._utilization[task.name].append(input_size, task)

    def plot_attribute(self, attribute):
        # get number of plots to make
        nplots = len(self.utilization)
        # make some scatter plots with the amounts vs input sizes
        ag = grid.AxesGrid(nplots, figsize=(10, 10))

        def plot_function(x, y, dependent_var, ax):
            ax.plot(x, y, c='royalblue', marker='o', markersize=10, markercolor='indianred')
            ax.xlabel('input_size')
            ax.ylabel(dependent_var)

        # build argument groups
        args = []
        for task_name, results in self.utilization.items():
            sizes, results = zip(*sorted(results, key=itemgetter(0)))
            args.append((sizes, [getattr(r, attribute) for r in results], task_name))

        ag.plot_all(args, plot_function)




