from tempfile import TemporaryFile


def split_google_storage_path(path):
    if not path.startswith('gs://'):
        raise ValueError('%s path is not a valid code review')
    prefix, _, bucket, *blob = path.split('/')
    return bucket, '/'.join(blob)


class ResourceUtilization:
    def __init__(self, task_name, open_log_file_object):
        self._open_log_file_object = open_log_file_object
        self._task_name = task_name

        # parse the file
        total_memory = 0
        total_disk = 0
        max_memory = 0
        max_disk = 0
        i = 0
        for i, line in enumerate(self._open_log_file_object):
            line = line.lower()  # make a bit more robust to capitalization in script
            if line.startswith(b'total memory (mb):'):
                total_memory = int(line.split()[-1])
            elif line.startswith(b'total disk space (kb):'):
                total_disk = int(line.split()[-1])
            elif line.startswith(b'* memory usage (mb):'):
                max_memory = max(max_memory, int(line.split()[-1]))
            elif line.startswith(b'* disk usage (kb):'):
                max_disk = max(max_disk, int(line.split()[-1]))
        robust_estimate = True if i >= 5 else False

        self.max_memory = max_memory
        self.total_memory = total_memory
        self.max_disk = max_disk
        self.total_disk = total_disk
        self.robust_estimate = robust_estimate
        self.fraction_disk_used = max_disk / total_disk
        self.fraction_memory_used = max_memory / total_memory

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
                task_name=self._task_name,
                max_memory=self.max_memory,
                total_memory=self.total_memory,
                max_disk=self.max_disk,
                total_disk=self.total_disk,
                robust=self.robust_estimate,
                disk_utilization=self.fraction_disk_used,
                memory_utilization=self.fraction_memory_used
            )
        )

    def __bytes__(self):
        return self.__str__().encode()

    def merge(self, other):
        if not isinstance(other, ResourceUtilization):
            raise TypeError(
                'Merge is only possible with other ResourceUtilization Objects')
        self.max_memory = max(self.max_memory, other.max_memory)
        self.total_memory = max(self.total_memory, other.total_memory)
        self.max_disk = max(self.max_disk, other.max_disk)
        self.total_disk = max(self.total_disk, other.total_disk)
        self.robust_estimate = any([self.robust_estimate, other.robust_estimate])
        self.fraction_disk_used = self.max_disk / self.total_disk
        self.fraction_memory_used = self.max_memory / self.total_memory


class Task:

    def __init__(self, metadata, client):
        self._metadata = metadata
        self._resource_utilization = None
        self.extract_resource_usage_information(client=client)

    @property
    def resource_utilization(self):
        return self._resource_utilization

    @resource_utilization.setter
    def resource_utilization(self, value):
        if not isinstance(value, ResourceUtilization):
            raise TypeError('resource utilization must be of a ResourceUtilization class')
        if self._resource_utilization is None:
            self._resource_utilization = value
        else:
            self._resource_utilization.merge(value)

    def _iterate_resource_utilization_logs(self, client):
        """check if there are any monitoring logs associated with this task

        :param client: authenticated google storage client
        :return:
        """
        log_names = [shard['monitoringLog'] for shard in self._metadata]
        for log_name in log_names:
            bucket_name, blob_name = split_google_storage_path(log_name)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            if blob.exists():
                yield blob

    def extract_resource_usage_information(self, client):
        for blob in self._iterate_resource_utilization_logs(client=client):
            with TemporaryFile() as fileobj:
                blob.download_to_file(fileobj)
                fileobj.seek(0)
                self._resource_utilization = ResourceUtilization(
                    task_name=self._metadata[0]['labels']['wdl-task-name'],
                    open_log_file_object=fileobj)
