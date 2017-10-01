from itertools import zip_longest, groupby
from collections import defaultdict
from functools import reduce
from scsequtil.plot import grid
from operator import itemgetter, attrgetter
from cromwell_tools.resource_utilization import ResourceUtilization


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
            robust = True if result[7].split()[-1] == 'True' else False
            resource_utilization = ResourceUtilization(
                task_name, max_mem, available_mem, max_disk, available_disk, robust)

            yield resource_utilization

    def aggregate(self):
        """aggregate resource utilization by task"""

        self._utilization = defaultdict(list)

        for input_size, log in self.logs.items():
            with open(log, 'r') as f:
                tasks = sorted(list(self.parse(f)), key=attrgetter('task_name'))
                for name, group in groupby(tasks, key=attrgetter('task_name')):
                    merged = reduce(ResourceUtilization.merge, group)
                    self._utilization[name].append((input_size, merged))

    def plot_attribute(self, attribute, remove_constant=True, *args, **kwargs):
        # get number of plots to make
        nplots = len(self.utilization)
        # make some scatter plots with the amounts vs input sizes
        ag = grid.AxesGrid(nplots, *args, **kwargs)

        def plot_function(x, y, dependent_var, ax):
            ax.loglog(
                x, y, c='royalblue', marker='o', markersize=10,
                markerfacecolor='indianred')
            ax.set_xlabel('input_size')
            ax.set_ylabel(dependent_var)

        # build argument groups
        args = []
        for task_name, results in self.utilization.items():
            sizes, results = zip(*sorted(results, key=itemgetter(0)))
            usage = [getattr(r, attribute) for r in results]
            if remove_constant:
                usage = [u - min(usage) + 1 for u in usage]
            args.append((sizes, usage, task_name))

        ag.plot_all(args, plot_function)
        return ag




