

class ResourceUtilization:
    """Class to store resource utilization information for a task, run on Cromwell."""

    def __init__(self, task_name, max_memory, total_memory, max_disk, total_disk, robust):
        """
        :param int task_name:
        :param int max_memory:
        :param int total_memory:
        :param int max_disk:
        :param int total_disk:
        :param bool robust:
        """
        self.task_name = task_name
        self.max_memory = max_memory
        self.total_memory = total_memory
        self.max_disk = max_disk
        self.total_disk = total_disk
        self.robust = robust
        self.fraction_disk_used = max_disk / total_disk
        self.fraction_memory_used = max_memory / total_memory

    @classmethod
    def from_file(cls, task_name, open_log_file_object):
        """Create a ResourceUtilization object from a monitoring log file.

        :param str task_name: Name of this task
        :param file open_log_file_object: an open monitoring log from cromwell
        :return ResourceUtilization: memory and disk utilization for this task
        """

        # parse the file
        total_memory, total_disk, max_memory, max_disk = 0, 0, 0, 0
        i = 0
        for i, line in enumerate(open_log_file_object):
            line = line.lower()  # make a bit more robust to capitalization in script
            if line.startswith(b'total memory (mb):'):
                total_memory = int(line.split()[-1])
            elif line.startswith(b'total disk space (kb):'):
                total_disk = int(line.split()[-1])
            elif line.startswith(b'* memory usage (mb):'):
                max_memory = max(max_memory, int(line.split()[-1]))
            elif line.startswith(b'* disk usage (kb):'):
                max_disk = max(max_disk, int(line.split()[-1]))
        robust = True if i >= 5 else False

        return cls(task_name, max_memory, total_memory, max_disk, total_disk, robust)

    def __str__(self):
        return (
            "{task_name} Monitoring Summary:\n"
            "Max Memory Usage (MB): {max_memory!s}\n"
            "Available Memory (MB): {total_memory!s}\n"
            "Max disk usage   (KB): {max_disk!s}\n"
            "Available disk   (KB): {total_disk}\n"
            "Disk Utilized     (%): {disk_utilization:.3f}\n"
            "Memory Utilized   (%): {memory_utilization:.3f}\n"
            "Robust Estimate?     : {robust}\n".format(
                task_name=self.task_name,
                max_memory=self.max_memory,
                total_memory=self.total_memory,
                max_disk=self.max_disk,
                total_disk=self.total_disk,
                robust=self.robust,
                disk_utilization=self.fraction_disk_used,
                memory_utilization=self.fraction_memory_used
            )
        )

    def __bytes__(self):
        return self.__str__().encode()

    @staticmethod
    def merge(x, y=None):
        """Merge two ResourceUtilization objects for the same task, returning the maximum
        utilization.

        :param ResourceUtilization x:
        :param ResourceUtilization y:
        :return ResourceUtilization: maximum resource utilization
        """
        if not all(isinstance(v, ResourceUtilization) or v is None for v in (x, y)):
            raise TypeError(
                'Merge is only possible between ResourceUtilization Objects, not %s and'
                ' %s' % (x, y))
        if y is None:
            return x
        else:
            max_memory = max(x.max_memory, y.max_memory)
            total_memory = max(x.total_memory, y.total_memory)
            max_disk = max(x.max_disk, y.max_disk)
            total_disk = max(x.total_disk, y.total_disk)
            robust = any([x.robust, y.robust])
            return ResourceUtilization(
                x.task_name, max_memory, total_memory, max_disk, total_disk, robust)
