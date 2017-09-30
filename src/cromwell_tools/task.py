from tempfile import TemporaryFile
from cromwell_tools.resource_utilization import ResourceUtilization


def split_google_storage_path(path):
    if not path.startswith('gs://'):
        raise ValueError('%s path is not a valid code review')
    prefix, _, bucket, *blob = path.split('/')
    return bucket, '/'.join(blob)


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
        else:  # merge with existing data
            self._resource_utilization = ResourceUtilization.merge(
                self._resource_utilization, value)

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
                self.resource_utilization = ResourceUtilization.from_file(
                    task_name=self._metadata[0]['labels']['wdl-task-name'],
                    open_log_file_object=fileobj)
