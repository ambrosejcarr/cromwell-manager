from tempfile import TemporaryFile
from cromwell_tools.resource_utilization import ResourceUtilization


def split_google_storage_path(path):
    """Utility to split a google storage path into bucket + key.

    :param str path: google storage path (must have gs:// prefix)
    :return str: bucket
    :return str: key
    """
    if not path.startswith('gs://'):
        raise ValueError('%s path is not a valid code review')
    prefix, _, bucket, *blob = path.split('/')
    return bucket, '/'.join(blob)


class Task:
    """Object to define an instance of a workflow task."""

    def __init__(self, metadata, client):
        """
        :param dict metadata: json dictionary of metadata for this task
        :param google.cloud.storage.Client client: Authenticate google storage client
        """
        self._metadata = metadata

        for blob in self._iterate_resource_utilization_logs(client=client):
            with TemporaryFile() as fileobj:
                blob.download_to_file(fileobj)
                fileobj.seek(0)
                self._resource_utilization = ResourceUtilization.from_file(
                    task_name=self._metadata[0]['labels']['wdl-task-name'],
                    open_log_file_object=fileobj)

    def _iterate_resource_utilization_logs(self, client):
        """Check if there are any monitoring logs associated with this task.

        :param client: authenticated google storage client
        :return Iterator: iterator over google storage blobs
        """
        log_names = [shard['monitoringLog'] for shard in self._metadata]
        for log_name in log_names:
            bucket_name, blob_name = split_google_storage_path(log_name)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            if blob.exists():
                yield blob

    @property
    def resource_utilization(self):
        """Task resource utilization.

        :return cromwell_tools.resource_utilization.ResourceUtilization: Resource utilization for
         this task
        """
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
